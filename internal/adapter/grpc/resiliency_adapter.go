package grpc

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"

	dresl "github.com/tijanadmi/my-grpc-go-server/internal/application/domain/resiliency"
	resl "github.com/tijanadmi/my-grpc-proto/protogen/go/resiliency"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func generateErrStatus(statusCode uint32) error {
	str := "Generated by server"

	if sc, ok := dresl.StatusCodeMap[statusCode]; ok && sc == codes.OK {
		return nil
	}

	if sc, ok := dresl.StatusCodeMap[statusCode]; ok {
		return status.New(sc, str).Err()
	} else {
		return status.New(codes.Internal, str).Err()
	}
}

func (a *GrpcAdapter) UnaryResiliency(ctx context.Context, req *resl.ResiliencyRequest) (
	*resl.ResiliencyResponse, error) {
	log.Println("UnaryResiliency called")
	str, sts := a.resiliencyService.GenerateResiliency(req.MinDelaySecond,
		req.MaxDelaySecond, req.StatusCodes)

	if errStatus := generateErrStatus(sts); errStatus != nil {
		return nil, errStatus
	}

	return &resl.ResiliencyResponse{
		DummyString: str,
	}, nil
}

func (a *GrpcAdapter) ServerStreamingResiliency(req *resl.ResiliencyRequest,
	stream resl.ResiliencyService_ServerStreamingResiliencyServer) error {
	log.Println("ServerStreamingResiliency called")

	context := stream.Context()

	for {
		select {
		case <-context.Done():
			log.Println("Client cancelled request")
			return nil
		default:
			str, sts := a.resiliencyService.GenerateResiliency(req.MinDelaySecond,
				req.MaxDelaySecond, req.StatusCodes)

			if errStatus := generateErrStatus(sts); errStatus != nil {
				return errStatus
			}

			stream.Send(&resl.ResiliencyResponse{
				DummyString: str,
			})
		}
	}
}

func (a *GrpcAdapter) ClientStreamingResiliency(
	stream resl.ResiliencyService_ClientStreamingResiliencyServer) error {
	log.Println("ClientStreamingResiliency called")

	i := 0

	for {
		req, err := stream.Recv()

		if err == io.EOF {
			res := resl.ResiliencyResponse{
				DummyString: fmt.Sprintf("Received %v requests from client", strconv.Itoa(i)),
			}

			return stream.SendAndClose(&res)
		}

		if req != nil {
			_, sts := a.resiliencyService.GenerateResiliency(req.MinDelaySecond,
				req.MaxDelaySecond, req.StatusCodes)

			if errStatus := generateErrStatus(sts); errStatus != nil {
				return errStatus
			}
		}

		i = i + 1
	}
}

func (a *GrpcAdapter) BiDirectionalResiliency(
	stream resl.ResiliencyService_BiDirectionalResiliencyServer) error {
	log.Println("BiDirectionalResiliency called")

	context := stream.Context()

	for {
		select {
		case <-context.Done():
			log.Println("Client cancelled request")
			return nil
		default:
			req, err := stream.Recv()

			if err == io.EOF {
				return nil
			}

			if err != nil {
				log.Fatalln("Error while reading from client :", err)
			}

			str, sts := a.resiliencyService.GenerateResiliency(req.MinDelaySecond,
				req.MaxDelaySecond, req.StatusCodes)

			if errStatus := generateErrStatus(sts); errStatus != nil {
				return errStatus
			}

			err = stream.Send(
				&resl.ResiliencyResponse{
					DummyString: str,
				},
			)

			if err != nil {
				log.Fatalln("Error while sending response to client :", err)
			}
		}
	}
}
