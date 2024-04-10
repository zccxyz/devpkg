package grpcx

import (
	"google.golang.org/grpc"
	"net"
)

type Server struct {
	Addr string
	*grpc.Server
}

func (s *Server) Serve() error {
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Server.Serve(l)
}
