package raft

import (
	"math/rand"
	"sync"
	"time"
	"trab2/labrpc" // trab2 é o módulo criado
)

// Variáveis globais
var TimeOutHB = 250     // Tempo limite para esperar o próximo heartbeat
var TimeBetweenHB = 200 // Tempo entre heartbeats

// Inicializar as contantes de estados para os nós Raft
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct { // Log do Raft --> registros de comandos
	LogTerm int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	// Parte adicionada
	currentTerm   int
	votedFor      int
	log           []LogEntry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	state         State
	grantVoteChan chan bool
	heartBeatChan chan bool
	leaderChan    chan bool
	commitChan    chan bool
}

// Estruturas usadas para chamadas RPC
type RequestVoteArgs struct {
	Term         int // Termo do candidato que está solicitando votos
	CandidateID  int // ID exclusivo do candidato que está solicitando votos
	LastLogIndex int // Índice do último registro no log do candidato
	LastLogTerm  int // Termo do último registro no log do candidato
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Nem todos os atributos são necessário para esses testes, mas estão na documentação
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	ReplyIndex int
}

// Usada em Make() para comunicação entre pares
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

func (rf *Raft) GetState() (int, bool) { // Retorna o termo atual do servidor e se ele acredita que é o líder
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Fase de eleição
// Esta função é responsável por lidar com as solicitações de votos
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()         // Concorrência --> bloqueio mutex
	defer rf.mu.Unlock() // Desbloqueio do mutex

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && args.LastLogTerm >= rf.log[len(rf.log)-1].LogTerm && args.LastLogIndex >= len(rf.log)-1 {
		rf.grantVoteChan <- true
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.state = Follower
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// Como esta função possui mais de um momento de retorno, utiliza-se o defer para evitar escrever Unlock() mais de uma vez
	// Isto também garante que todo o escopo da função seja uma seção crítica
	defer rf.mu.Unlock() // Executa no final do escopo atual

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	rf.heartBeatChan <- true

	reply.Success = true
	reply.ReplyIndex = len(rf.log)
	return
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != Leader {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			return ok
		}
	}
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Nenhum código é necessário aqui.
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}            // Nova instância de Raft
	rf.peers = peers         // Servidores pares
	rf.persister = persister // Salva e carrega estados
	rf.me = me               // Identificador do servidor

	rf.votedFor = -1
	rf.grantVoteChan = make(chan bool, 100)
	rf.heartBeatChan = make(chan bool, 100)
	rf.leaderChan = make(chan bool, 100)
	rf.commitChan = make(chan bool, 100)
	rf.log = append(rf.log, LogEntry{LogTerm: 0})

	rf.lastApplied = 0

	go func() {
		for {
			switch rf.state {
			case Follower:
				select {
				case <-rf.heartBeatChan: // Se não receber até o limite de tempo, significa que não há líder
				case <-time.After(time.Duration(rand.Intn(TimeOutHB)+TimeOutHB) * time.Millisecond):
					rf.state = Candidate
				}
			case Leader: // Motivo do warning
				rf.mu.Lock()

				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderID = rf.me

				rf.mu.Unlock()

				for i := range rf.peers { // Mandar heartbeat para todos os pares
					if i != rf.me && rf.state == Leader {

						go func(i int) {
							var reply AppendEntriesReply
							rf.sendAppendEntries(i, args, &reply) // Remover causa Warning
						}(i)
					}
				}
				time.Sleep(time.Duration(TimeBetweenHB) * time.Millisecond) // Tempo que irá esperar para enviar o próximo heartbeat
			case Candidate:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				var voteCount int = 1

				var args RequestVoteArgs
				args.Term = rf.currentTerm
				args.CandidateID = rf.me

				rf.mu.Unlock()
				for i := range rf.peers {
					// Percorrer todos os outros nós
					if i != rf.me {
						go func(i int) {
							// Verificar em quem outro nó votou, vamos chamá-lo de "eleitor" para evitar confusão
							var reply RequestVoteReply
							rf.sendRequestVote(i, args, &reply)

							if reply.VoteGranted == true {
								// O eleitor votou neste nó
								rf.mu.Lock()
								voteCount++
								if voteCount > len(rf.peers)/2 && rf.state == Candidate {
									// O nó é candidato e venceu a eleição
									rf.leaderChan <- true
								}
								rf.mu.Unlock()
							} else if reply.Term > rf.currentTerm {
								// O eleitor não votou neste nó e possui um termo superior, então este nó deixa de ser candidato e vira seguidor
								rf.mu.Lock()
								rf.currentTerm = reply.Term
								rf.state = Follower
								rf.votedFor = -1
								rf.mu.Unlock()
							}
						}(i)
					}
				}
				select {
				case <-time.After(time.Duration(rand.Intn(TimeOutHB)+TimeOutHB) * time.Millisecond):
				case <-rf.heartBeatChan:
					rf.mu.Lock()
					rf.state = Follower
					rf.mu.Unlock()
				case <-rf.leaderChan:
					rf.mu.Lock()
					rf.state = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}
		}
	}()

	return rf
}
