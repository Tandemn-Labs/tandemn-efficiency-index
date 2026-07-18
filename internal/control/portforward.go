package control

import (
	"bytes"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"time"
)

type PortForward struct {
	Namespace  string
	Service    string
	LocalPort  int
	RemotePort int
	Timeout    time.Duration

	command *exec.Cmd
	stderr  bytes.Buffer
	done    chan error
}

func (p *PortForward) Start() error {
	if p.LocalPort == 0 {
		port, err := availablePort()
		if err != nil {
			return err
		}
		p.LocalPort = port
	}

	p.command = exec.Command(
		"kubectl",
		"--namespace",
		p.Namespace,
		"port-forward",
		"service/"+p.Service,
		fmt.Sprintf("%d:%d", p.LocalPort, p.RemotePort),
	)
	p.command.Stderr = &p.stderr
	if err := p.command.Start(); err != nil {
		return fmt.Errorf("start kubectl port-forward: %w", err)
	}
	p.done = make(chan error, 1)
	go func() {
		p.done <- p.command.Wait()
		close(p.done)
	}()

	deadline := time.Now().Add(p.Timeout)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			return fmt.Errorf("kubectl port-forward exited: %s", p.stderr.String())
		default:
		}
		connection, err := net.DialTimeout(
			"tcp",
			net.JoinHostPort("127.0.0.1", strconv.Itoa(p.LocalPort)),
			200*time.Millisecond,
		)
		if err == nil {
			connection.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	p.Stop()
	return fmt.Errorf("timed out waiting for kubectl port-forward")
}

func (p *PortForward) URL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", p.LocalPort)
}

func (p *PortForward) Stop() {
	if p.command == nil || p.command.Process == nil {
		return
	}
	if p.command.ProcessState == nil || !p.command.ProcessState.Exited() {
		_ = p.command.Process.Kill()
	}
	if p.done != nil {
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
		}
	}
}

func availablePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("allocate local port: %w", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
