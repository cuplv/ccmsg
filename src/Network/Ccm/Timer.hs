module Network.Ccm.Timer
  ( Timer
  , forkTimer
  , awaitTimer
  , tryTimer
  , cancelTimer
  ) where

import Control.Monad.IO.Class
import Control.Concurrent
import Control.Concurrent.STM

data Timer
  = Timer
    { timerVar :: TMVar ()
    , timerThread :: ThreadId
    }

{- | Create a timer that is counting down the given number of
   microseconds -}
forkTimer :: (MonadIO m) => Int -> m Timer
forkTimer micros = do
  v <- liftIO newEmptyTMVarIO
  tid <- liftIO . forkIO $ do
    threadDelay micros
    atomically $ putTMVar v ()
  return (Timer v tid)

{- | An 'STM' action that blocks until the timer expires.  After the
   timer has expired, 'awaitTimer' and 'tryTimer' will return
   successfully repeatedly. -}
awaitTimer :: Timer -> STM ()
awaitTimer t = readTMVar (timerVar t)

{- | An 'STM' action that checks whether the timer has expired, without
   blocking. -}
tryTimer :: Timer -> STM Bool
tryTimer t = do
  r <- tryReadTMVar (timerVar t)
  case r of
    Just () -> return True
    Nothing -> return False

{- | Kill the timer thread. -}
cancelTimer :: (MonadIO m) => Timer -> m ()
cancelTimer t = liftIO $ killThread (timerThread t)
