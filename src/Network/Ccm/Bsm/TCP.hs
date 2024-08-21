module Network.Ccm.Bsm.TCP where

import Network.Ccm.Bsm.Internal
import Network.Ccm.Lens
import Network.Ccm.Types

import Control.Concurrent (forkIO,threadDelay)
import Control.Concurrent.STM
import Control.Exception (catch, IOException, SomeException)
import Control.Monad (forever)
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import Data.Map (Map)
import Data.Word (Word64)
import qualified Data.Map as Map
import qualified Data.Store as Store
import Network.Simple.TCP

type MsgSize = Int

sizeMsgSize :: Int
sizeMsgSize = case (Store.size :: Store.Size MsgSize) of
  Store.VarSize _ -> error "MsgSize has variable Store size"
  Store.ConstSize i -> i

data MyAddr
  = MyAddr { myAddrHost :: HostName
           , myAddrPort :: ServiceName
           }
  deriving (Show)

runServer :: MyAddr -> Bsm -> IO ()
runServer myaddr bsm = do
  debug (bsm^.bsmDbg) "Opening server..."
  serve (Host (myAddrHost myaddr)) (myAddrPort myaddr) $ \(sock,addr) -> do
    r <- acceptConnection (bsm^.bsmDbg) sock addr
    case r of
      Just target -> case Map.lookup target (bsm^.bsmPeers) of
        Just p -> do
          forkIO $ runSender (bsm^.bsmDbg) target (p^.peerOutbox) sock
          atomically $ writeTVar (p^.peerStatus) PSConnected
          catch (runReceiver (bsm^.bsmDbg) target (bsm^.bsmInbox) sock) $ \e -> do
            error $ "Caught: " ++ show (e :: IOException)
        Nothing -> debug (bsm^.bsmDbg) $ "No outbox for " ++ show target
      Nothing -> return ()

-- testServer = do
--   c <- newBsmInbox
--   out <- newBsmOutbox
--   runServer mkPrinterDbg (MyAddr "127.0.0.1" "8099") c out
--   putStrLn "Test..."

acceptConnection
  :: Debugger
  -> Socket
  -> SockAddr
  -> IO (Maybe NodeId)
acceptConnection dbg sock addr = do
  bs <- recvUntil sock nodeIdSize
  case bs of
    Just bs -> do
      let nid = Store.decodeEx bs
      debug dbg $ "Accepted connection from " ++ show nid
      return (Just nid)
    Nothing -> do
      debug dbg $ "Failed to init connection from " ++ show addr
      return Nothing

runReceiver
  :: Debugger
  -> NodeId
  -> TBQueue (NodeId, ByteString)
  -> Socket
  -> IO ()
runReceiver dbg i inbox sock = do
  bs <- recvUntil sock sizeMsgSize
  case bs of
    Just bs -> do
      let n = Store.decodeEx bs
      bs2 <- recvUntil sock $ fromIntegral (n :: MsgSize)
      case bs2 of
        Just bs2 -> do
          atomically $ writeTBQueue inbox (i,bs2)
          let s = Store.decodeEx bs2
          debug dbg $ "[" ++ show i ++ "]: " ++ s
          runReceiver dbg i inbox sock
        Nothing ->
          debug dbg $ "Connection closed (mid-msg): " ++ show i
    Nothing ->
      debug dbg $ "Connection closed: " ++ show i

runSender :: Debugger -> NodeId -> TBQueue PeerMessage -> Socket -> IO ()
runSender d i outbox sock = do
  PMData bs <- atomically $ readTBQueue outbox
  let n = ByteString.length bs
  let sendAction = do
        send sock (Store.encode (n :: MsgSize) <> bs)
        debug d $ "Sent " ++ show n ++ "b message to " ++ show i  
  catch sendAction $ \e ->
    debug d $ "[Send] Caught: " ++ show (e :: SomeException)
  runSender d i outbox sock

recvUntil :: Socket -> Int -> IO (Maybe ByteString)
recvUntil sock n = do
  bs <- recv sock n
  case bs of
    Just bs | ByteString.length bs > n -> error "recvUntil: Too many bytes?"
    Just bs | ByteString.length bs == n -> return $ Just bs
    Just bs | ByteString.length bs < n ->
      fmap (fmap (bs <>)) (recvUntil sock (n - ByteString.length bs))
    Nothing -> return Nothing

runClient
  :: Int
  -> Bsm
  -> NodeId
  -> MyAddr
  -> IO ()
runClient delay bsm targetNode targetAddr = do
  let peer = case Map.lookup targetNode (bsm^.bsmPeers) of
        Just p -> p
        Nothing -> error $ "runClient on non-existent peer: " ++ show targetNode
  forever $
    catch (runClient' bsm targetNode targetAddr peer) $ \e -> do
      -- putStrLn $ "Caught: " ++ show (e :: IOException)
      let _ = e :: IOException
      threadDelay delay
      -- putStrLn $ "Retrying connection to " ++ show targetNode
      -- runClient (delay * 2) dbg selfNode targetNode targetAddr peer inbox

runClient'
  :: Bsm
  -> NodeId
  -> MyAddr
  -> Peer
  -> IO ()
runClient' bsm targetNode (MyAddr host port) peer =
  connect host port $ \(sock,_) -> do
    -- putStrLn $ "Connected to " ++ show targetNode
    send sock (Store.encode (bsm^.bsmSelf))
    forkIO $ runSender (bsm^.bsmDbg) targetNode (peer^.peerOutbox) sock
    atomically $ writeTVar (peer^.peerStatus) PSConnected
    runReceiver (bsm^.bsmDbg) targetNode (bsm^.bsmInbox) sock
