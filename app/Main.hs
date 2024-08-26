module Main where

import Config
import State

import qualified Network.Ccm as Ccm
import Network.Ccm.State (ccmStats,totalOutOfOrder)
import Network.Ccm.Lens

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Monad (filterM)
import Control.Monad.State
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Sequence as Seq
import qualified Data.Store as Store
import System.Environment (getArgs)
import System.Random
import Data.Time.Clock
import Data.Time.Format

main :: IO ()
main = do
  confFile <- getArgs >>= \as -> case as of
    [confFile] -> return confFile
    [] -> error "Called without config file argument"
    _ -> error "Called with more than one argument"
  conf <- inputConfig confFile
  putStrLn $ "Running " ++ show (conf ^. cNodeId)
  (errors,td) <- runExT nodeScript Ccm.mkNoDebugDbg conf
  putStrLn $ "Causal deferrals: " ++ show errors
  putStrLn $ "Finished in " ++ showResultSeconds td

showResultSeconds :: NominalDiffTime -> String
showResultSeconds = formatTime defaultTimeLocale "%3Ess"

atomicallyTimed :: (MonadIO m) => Int -> STM a -> m (Maybe a)
atomicallyTimed us m = do
  v <- liftIO $ newEmptyTMVarIO
  tid <- liftIO $ forkIO $ do
    threadDelay us
    atomically $ putTMVar v ()
  liftIO . atomically $
    (Just <$> m)
    `orElse`
    (const Nothing <$> takeTMVar v)

debug :: (MonadIO m) => String -> ExT m ()
debug s = do
  lvl <- use $ stConf . cDebugLevel
  if lvl > 0
    then liftIO $ putStrLn s
    else return ()

nodeScript :: ExT IO (Int, NominalDiffTime)
nodeScript = do
  ctx <- lift Ccm.context
  sto <- use $ stConf . cExpr . cSetupTimeout
  result <- case sto of
    Just ms -> atomicallyTimed (ms * 1000) (check =<< Ccm.allPeersReady ctx)
    Nothing -> liftIO.atomically $ Just <$> (check =<< Ccm.allPeersReady ctx)
  case result of
    Nothing -> error "Timeout during setup"
    Just () -> do
      t0 <- liftIO $ getCurrentTime
      self <- lift Ccm.getSelf
      others <- lift Ccm.getOthers
      debug $ "Send Loop"
      sendLoop
      debug $ "Recieve Loop"
      recvLoop
      t1 <- liftIO $ getCurrentTime
      let td = diffUTCTime t1 t0
      errors <- lift . use $ ccmStats . totalOutOfOrder

      liftIO $ threadDelay 1000000
      return (errors,td)

checkAllDone :: (MonadIO m) => ExT m Bool
checkAllDone = do
  self <- lift Ccm.getSelf
  others <- Set.toList <$> lift Ccm.getOthers
  ocs <- use stReceived
  debug $ "Checking done: " ++ show ocs
  total <- use $ stConf . cExpr . cMsgCount
  next <- use $ stNextSend
  let
    f o = case Map.lookup o ocs of
      Just n -> n >= total - 1
      Nothing -> error $ "Who is node " ++ show o ++ "?"
    allReceived = and (map f others)
  if not allReceived
    then debug $ "Not done."
    else debug $ "Done."
  return $ allReceived

tryRecv :: ExT IO ()
tryRecv = do
  msgs <- lift Ccm.tryRecv
  let msgs' = fmap Store.decodeEx msgs
  accMsgs msgs'
  debug $ "Got " ++ show msgs'

timedRecv :: ExT IO Bool
timedRecv = do
  ctx <- lift Ccm.context
  rto <- use $ stConf . cExpr . cRecvTimeout
  case rto of
    Just ms -> do
      new <- atomicallyTimed (ms * 1000) $
        check =<< Ccm.newNetworkActivity ctx
      case new of
        Just () -> tryRecv >> return False
        Nothing -> return True
    Nothing -> tryRecv >> return False

randomSendTarget :: Int -> ExT IO Ccm.SendTarget
randomSendTarget denom = do
  partial <- randomRIO (0,denom)
  if partial == (0 :: Int)
    then do
      peers <- Set.toList <$> lift Ccm.getOthers
      peers' <- filterM (\_ -> randomIO) peers
      return $ Ccm.SendTo (Set.fromList peers')
    else return Ccm.SendAll

sendLoop :: ExT IO ()
sendLoop = do
  self <- lift Ccm.getSelf
  total <- use $ stConf . cExpr . cMsgCount
  drop <- use $ stConf . cExpr . cDropMessages
  next <- use $ stNextSend
  if next >= total
    then return ()
    else do
      target <- case drop of
        Just n -> randomSendTarget n
        Nothing -> return Ccm.SendAll
      lift $ Ccm.blockSendPartial target (Store.encode (self,next))
      stNextSend += 1
      tryRecv
      liftIO $ threadDelay 1000
      sendLoop

recvLoop = do
  continue <- not <$> checkAllDone
  if continue
    then do
      timedOut <- timedRecv
      if timedOut
        then do
          rs <- use stReceived
          error $ "Timeout during experiment: " ++ show rs
        else
          recvLoop
    else
      return ()

accMsgs :: (Monad m) => Seq.Seq ExMsg -> ExT m ()
accMsgs Seq.Empty = return ()
accMsgs ((sender,n) Seq.:<| ms) = do
  stReceived . at sender %= \v -> case v of
    Just n' | n' > n -> Just n'
    _ -> Just n
  accMsgs ms
