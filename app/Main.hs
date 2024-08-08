module Main where

import Config
import State

import qualified Network.Ccm as Ccm
import Network.Ccm.State (ccmStats,totalOutOfOrder)
import Network.Ccm.Lens

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Monad.State
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Sequence as Seq
import qualified Data.Store as Store
import System.Environment (getArgs)
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
      sendLoop
      recvLoop
      -- Wait until all sent messages are actually transmitted.
      liftIO . atomically $ check =<< Ccm.sendsComplete ctx
      t1 <- liftIO $ getCurrentTime
      let td = diffUTCTime t1 t0
      errors <- lift . use $ ccmStats . totalOutOfOrder
      return (errors,td)

checkAllDone :: (Monad m) => ExT m Bool
checkAllDone = do
  self <- lift Ccm.getSelf
  others <- Set.toList <$> lift Ccm.getOthers
  ocs <- use stReceived
  total <- use $ stConf . cExpr . cMsgCount
  next <- use $ stNextSend
  let
    f o = case Map.lookup o ocs of
      Just n -> n >= total - 1
      Nothing -> total == 0
    allSent = next >= total
    allReceived = and (map f others)
  return $ allSent && allReceived

tryRecv :: ExT IO ()
tryRecv = do
  msgs <- lift Ccm.tryRecv
  accMsgs (fmap Store.decodeEx msgs)

timedRecv :: ExT IO Bool
timedRecv = do
  ctx <- lift Ccm.context
  rto <- use $ stConf . cExpr . cRecvTimeout
  case rto of
    Just ms -> do
      new <- atomicallyTimed (ms * 1000) $
        check =<< Ccm.newNetworkActivity ctx
      case new of
        Just () -> tryRecv >> return True
        Nothing -> return False
    Nothing -> tryRecv >> return True

sendLoop :: ExT IO ()
sendLoop = do
  self <- lift Ccm.getSelf
  total <- use $ stConf . cExpr . cMsgCount
  next <- use $ stNextSend
  if next >= total
    then return ()
    else do
      lift $ Ccm.blockSend (Store.encode (self,next))
      stNextSend += 1
      tryRecv
      -- threadDelay 1000
      sendLoop

recvLoop = do
  continue <- checkAllDone
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

-- nodeLoop :: ExT IO ()
-- nodeLoop = checkAllDone >>= \r -> if r
--   then do
--     return ()
--   else do
--     rto <- use $ stConf . cExpr . cRecvTimeout
--     next <- use stNextSend
--     limit <- use $ stConf . cExpr . cMsgCount

--     result <- lift . atomicallyCcmTimedMicros (fmap (* 1000) rto) $
--       orElseCcm nodeRecv (nodeSend next limit)

--     case result of
--       Nothing -> do
--         rs <- use stReceived
--         error $ "Timeout during experiment, " ++ show rs
--       Just (Sent _) -> do
--         stNextSend += 1
--       Just (Received msgs) -> do
--         accMsgs msgs
--       Just (ReceivedError e) -> do
--         stSeenErrors += 1
--         self <- lift getSelf
--         liftIO . putStrLn $ showCausalError' self e

--     nodeLoop

accMsgs :: (Monad m) => Seq.Seq ExMsg -> ExT m ()
accMsgs Seq.Empty = return ()
accMsgs ((sender,n) Seq.:<| ms) = do
  stReceived . at sender %= \v -> case v of
    Just n' | n' > n -> Just n'
    _ -> Just n
  accMsgs ms

-- data ActionResult
--   = Sent Int
--   | Received [ExMsg]
--   | ReceivedError Ccm.CausalError

-- nodeRecv :: Ccm.CcmT STM ActionResult
-- nodeRecv = do
--   undefined
--   -- result <- (fmap . map $ Store.decodeEx) <$> recvCcm
--   -- case result of
--   --   Right msgs ->
--   --     return $ Received msgs
--   --   Left e ->
--   --     return $ ReceivedError e

-- nodeSend :: Int -> Int -> Ccm.CcmT STM ActionResult
-- nodeSend next total | next >= total = stmCcm retry
-- nodeSend next _ = do
--   self <- Ccm.getSelf
--   let msg = (self, next)
--   Ccm.blockSend (Store.encode msg)
--   return $ Sent next
