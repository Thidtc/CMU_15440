// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
    "bufio"
    "fmt"
    "net"
    "strconv"
    "strings"
)

const MAX_WRITE_BUFFER_SIZE = 500

type keyValueServer struct {
    listener net.Listener
    clis map[client]int
    n_dead_cli int
    req chan request
    count_active chan int
    count_dropped chan int
    act_close chan int
    act_accept_conn chan net.Conn
    need_to_close chan client
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
    server := keyValueServer {
        listener: nil,
        clis: make(map[client]int),
        n_dead_cli: 0,
        req: make(chan request),
        count_active: make(chan int),
        count_dropped: make(chan int),
        act_close: make(chan int),
        act_accept_conn: make(chan net.Conn),
        need_to_close: make(chan client),
    }
    return &server
}

// Start server
func (kvs *keyValueServer) Start(port int) error {
    // Init DB
    init_db()
    addr := ":" + strconv.Itoa(port)
    var err error
    kvs.listener, err = net.Listen("tcp", addr)
    if err != nil {
        return err
    }
    // Accept routine
    go kvs.AcceptRoutine()
    // Main routine
    go kvs.MainRoutine()
    return nil
}

// Close server
func (kvs *keyValueServer) Close() {
    kvs.act_close <- 1
}

// Count alive connection
func (kvs *keyValueServer) Count() int {
    // Count cannot be called parallelly
    kvs.count_active <- 1
    return <- kvs.count_active
}

// Accept client connection routine
func (kvs *keyValueServer) AcceptRoutine() error {
    for {
        conn, err := kvs.listener.Accept()
        if err != nil {
            return err
        }
        kvs.act_accept_conn <- conn
    }
}

// Main routine
func (kvs *keyValueServer) MainRoutine() error {
    for {
        select {
        case <- kvs.act_close:
            // Close server and shutdown connection
            kvs.listener.Close()
            for c, _ := range kvs.clis {
                // Close client routine
                c.read_quit <- 1
                c.write_quit <- 1
                c.conn.Close()
                kvs.n_dead_cli++
            }
            return nil
        case cli := <- kvs.need_to_close:
            // Close client write routine
            cli.write_quit <- 1
            delete(kvs.clis, cli)
        case <- kvs.count_active:
            kvs.count_active <- len(kvs.clis)
        case <- kvs.count_dropped:
            kvs.count_dropped <- kvs.n_dead_cli
        case conn := <- kvs.act_accept_conn:
            new_cli := client {
                id: 0,
                conn: conn,
                server: kvs,
                write_chan: make(chan []byte, MAX_WRITE_BUFFER_SIZE),
                write_quit: make(chan int),
                read_quit: make(chan int),
            }
            kvs.clis[new_cli] = 1
            // Serve the connection
            go kvs.ConnReadRoutine(new_cli)
            go kvs.ConnWriteRoutine(new_cli)
        case req := <- kvs.req:
            // fmt.Println("Receive request: ", req)
            switch req.cmd {
            case "put":
                key, value := req.args[0], req.args[1]
                put(key, []byte(value))
            case "get":
                key := req.args[0]
                value := get(key)
                response := fmt.Sprintf("%s,%s\n", key, value)
                // Response to every client
                for cli, _ := range kvs.clis {
                    // cli.write_chan <- []byte(response)
                    if len(cli.write_chan) == MAX_WRITE_BUFFER_SIZE {
                        <- cli.write_chan
                    }
                    cli.write_chan <- []byte(response)
                }

            default:
                fmt.Printf("Command %s not supported", req.cmd)
            }
        }
    }
    return nil
}

// Read from client
func (kvs *keyValueServer) ConnReadRoutine(cli client) error {
    conn := cli.conn
    reader := bufio.NewReader(conn)
    for {
        content, err := reader.ReadBytes(byte('\n'))
        if err != nil {
            kvs.need_to_close <- cli
            return err
        }
        cmd, args := parse_request(string(content))
        kvs.req <- request {
            cmd: cmd,
            args: args,
        }
    }
}

// Write to client
func (kvs *keyValueServer) ConnWriteRoutine(cli client) error {
    for {
        select {
        case response := <- cli.write_chan:
            // fmt.Println("Server-write", cli, response)
            if _, err := cli.conn.Write(response); err != nil {
                return err
            }
        case <- cli.write_quit:
            return nil
        }
    }
}

func parse_request(request_str string) (string, []string) {
    tokens := strings.Split(strings.Trim(request_str, "\n"), ",")
    return tokens[0], tokens[1:]
}

// Request
type request struct {
    cmd string
    args []string
}

// Client
type client struct {
    id int
    conn net.Conn
    server *keyValueServer
    write_chan chan []byte
    write_quit chan int
    read_quit chan int
}
