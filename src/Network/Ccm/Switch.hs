module Network.Ccm.Switch
  ( Switch
  , passSwitch
  , takeSwitch
  , tryFlipSwitch
  , forkSwitch
  , forkRepeatSwitch
  , forkTimerSwitch
  ) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad (forever)
import Control.Monad.IO.Class

data Switch
  = Switch
    { inputVar :: TMVar ()
    , outputVar :: TMVar ()
    }

takeSwitch :: Switch -> STM ()
takeSwitch sw = takeTMVar (inputVar sw)

passSwitch :: Switch -> STM ()
passSwitch sw = putTMVar (outputVar sw) ()

{- | Try to take the 'Switch', without blocking.  If successful,
   immediately pass it again and return 'True'. -}
tryFlipSwitch :: Switch -> STM Bool
tryFlipSwitch sw = do
  result <- tryTakeTMVar (inputVar sw)
  case result of
    Just () -> do
      putTMVar (outputVar sw) ()
      return True
    Nothing ->
      return False

{- | Fork a thread that will use one end of the 'Switch' in an arbitrary
   way.

   By convention, the 'Switch' end that is returned to the parent
   thread begins in the "on" position, and the end that is given to
   the child begins in the "off" position, but this is not enforced. -}
forkSwitch :: (MonadIO m) => (Switch -> IO ()) -> m (ThreadId, Switch)
forkSwitch f = do
  childInput <- liftIO $ newEmptyTMVarIO
  parentInput <- liftIO $ newEmptyTMVarIO

  let
    childSwitch = Switch
      { inputVar = childInput
      , outputVar = parentInput
      }

    parentSwitch = Switch
      { inputVar = parentInput
      , outputVar = childInput
      }

  tid <- liftIO.forkIO $ f childSwitch

  return (tid, parentSwitch)

{- | Fork a thread that will repeat the given 'IO' action each time the
   switch is passed to it, passing the switch back when it is done.

   The switch end returned to the parent is in the "on" position: the
   child will not take its first action until the parent uses
   'passSwitch'. -}
forkRepeatSwitch :: (MonadIO m) => IO () -> m (ThreadId, Switch)
forkRepeatSwitch m = forkSwitch $ \sw -> do
  forever $ do
    atomically $ takeSwitch sw
    m
    atomically $ passSwitch sw

{- | Fork a thread that will wait for the given number of microseconds
   each time the switch is passed to it, passing the switch back when
   it is done.

   The switch end returned to the parent is in the "on" position: the
   child will not begin waiting until the parent uses 'passSwitch'. -}
forkTimerSwitch :: (MonadIO m) => Int -> m (ThreadId, Switch)
forkTimerSwitch micros = forkRepeatSwitch (threadDelay micros)
