package redrpc

type RPCTimeout struct {
	text string
}

func (e *RPCTimeout) Error() string {
	return e.text
}

type RemoteException struct {
	text string
}

func (e *RemoteException) Error() string {
	return e.text
}

func IsRPCTimeout(err error) bool {
	_, ok := err.(*RPCTimeout)
	return ok
}

func IsRemoteException(err error) bool {
	_, ok := err.(*RemoteException)
	return ok
}
