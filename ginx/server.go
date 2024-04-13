package ginx

import "github.com/gin-gonic/gin"

type Server struct {
	Server *gin.Engine
	Addr   string
}

func (s *Server) Start() error {
	return s.Server.Run(s.Addr)
}
