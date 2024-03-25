module Network.Ccm.Bsm.UDP where

import Network.Ccm.Bsm.Internal (bsmBoxBound)
import Network.Ccm.Types

import Control.Concurrent (forkIO,threadDelay)
import Control.Concurrent.STM
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IP (IP)
import Data.Map (Map)
import qualified Data.Map as Map
import qualified Data.Store as Store
import Data.Word (Word32)
import Network.UDP
import Network.Socket (getAddrInfo, SockAddr, PortNumber, AddrInfo(addrAddress))

data UAddr = UAddr IP PortNumber

data UInbox = UInbox (TBQueue (NodeId, ByteString))
data UOutbox = UOutbox (TBQueue (NodeId, ByteString))

runServer :: Debugger -> UAddr -> NodeId -> Map NodeId SockAddr -> UInbox -> UOutbox -> IO ()
runServer dbg (UAddr ip port) self nmap inbox outbox = do
  sock <- serverSocket (ip, port)
  let amap = Map.fromList . map (\(a,b) -> (b,a)) . Map.toList $ nmap
  forkIO $ runSender dbg self sock nmap outbox
  runReceiver dbg sock amap inbox

runSender :: Debugger -> NodeId -> ListenSocket -> Map NodeId SockAddr -> UOutbox -> IO ()
runSender dbg self sock nmap (UOutbox outbox) = do
  (n,bs) <- atomically $ readTBQueue outbox
  case Map.lookup n nmap of
    Just addr -> sendTo sock (Store.encode (nodeIdWord self) <> bs) (ClientSockAddr addr [])
    Nothing -> error $ "No address for " ++ show n
  runSender dbg self sock nmap (UOutbox outbox)

runReceiver :: Debugger -> ListenSocket -> Map SockAddr NodeId -> UInbox -> IO ()
runReceiver dbg sock amap (UInbox inbox) = do
  (bs, ClientSockAddr addr _) <- recvFrom sock
  debug dbg "Received msg?"
  let (nodeBs,msg) = BS.splitAt 8 bs
      node = NodeId $ Store.decodeEx nodeBs
  atomically $ writeTBQueue inbox (node,msg)
  runReceiver dbg sock amap (UInbox inbox)

testMap = Map.fromList
  [(NodeId 0,("127.0.0.1","8050"))
  ,(NodeId 1,("127.0.0.1","8051"))
  ]

sendLoop outbox target = do
  atomically $ writeTBQueue outbox (target, Store.encode "Hello world.")
  threadDelay 1000000
  sendLoop outbox target

recvLoop inbox = do
  (n,bs) <- atomically $ readTBQueue inbox
  let msg = Store.decodeEx bs
  putStrLn $ "Got msg from " ++ show n ++ ": " ++ msg
  recvLoop inbox

testServer :: Word32 -> Word32 -> IO ()
testServer self target = do
  let Just (myIp,myPort) = Map.lookup (NodeId self) testMap
  let uaddr = UAddr (read myIp) (read myPort)
  nmap <- Map.traverseWithKey (\_ -> mkSockAddr) testMap
  inb <- newTBQueueIO 100
  outb <- newTBQueueIO 100
  forkIO $ runServer mkPrinterDbg uaddr (NodeId self) nmap (UInbox inb) (UOutbox outb)
  forkIO $ sendLoop outb (NodeId target)
  recvLoop (inb :: TBQueue (NodeId, ByteString))

mkSockAddr :: (String,String) -> IO SockAddr
mkSockAddr (host,port) = do
  infos <- getAddrInfo Nothing (Just host) (Just port)
  case infos of
    (i:_) -> return $ addrAddress i
    [] -> error "No address."
