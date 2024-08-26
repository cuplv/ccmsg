{-# LANGUAGE LambdaCase #-}

module Network.Ccm.Algorithm
  ( processMessage
  ) where

import Network.Ccm.Lens
import Network.Ccm.State
import Network.Ccm.Types
import Network.Ccm.VClock

import Control.Monad (when)
import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Either (rights)
import Data.Foldable (foldlM,for_)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromJust,isJust)
import Data.Sequence (Seq,(<|),(|>))
import qualified Data.Sequence as Seq
import Data.Traversable (for)
import GHC.Exts (toList,fromList)

{- | Process a newly-received message, returning any accepted payloads
   that result. -}
processMessage
  :: (Monad m)
  => (NodeId, AppMsg)
  -> CcmST m (Seq ByteString)
processMessage (sender,msg) =
  (flip execStateT) Seq.Empty $ do
    tryAcceptFresh sender msg >>= \case
      Just mid -> acceptRec mid
      Nothing -> return ()

type CcmP m = StateT (Seq ByteString) (CcmST m)

acceptRec
  :: (Monad m)
  => MsgId
  -> CcmP m ()
acceptRec mid = do
  -- Get a sequence of node IDs, each representing a message from
  -- that node that has waiting on 'sender'.
  revs <- lift $ revive mid
  -- Try accepting a waiting message for each revived node ID,
  -- collecting any successfully accepted message IDs.
  delivs <- justSeq <$> for revs (\i -> tryAcceptDef i)
  -- Recurse on the successfully accepted message IDs.
  for_ delivs acceptRec

tryAcceptFresh
  :: (Monad m)
  => NodeId
  -> AppMsg
  -> CcmP m (Maybe MsgId)
tryAcceptFresh sender msg = tryAccept sender (Just msg)

tryAcceptDef
  :: (Monad m)
  => NodeId
  -> CcmP m (Maybe MsgId)
tryAcceptDef sender = tryAccept sender Nothing

tryAccept
  :: (Monad m)
  => NodeId
  -> Maybe AppMsg
  -> CcmP m (Maybe MsgId)
tryAccept sender mmsg = do
  let fresh = isJust mmsg
  msg <- case mmsg of
    Just msg -> return msg
    -- If no message was passed, we're supposed to get it from the
    -- sender's waiting queue.  We assume that this function is only
    -- called with 'Nothing' when the sender actually has a waiting
    -- message.
    Nothing -> lift $
      fromJust <$> preuse (cache sender . mcWaiting . ix 0)

  lift (punchClock sender (msg^.msgClock)) >>= \case
    ClockAccepted -> do
      when fresh . lift $
        ccmStats . totalInOrder += 1
      -- Remove the message from the front of the waiting queue
      when (not fresh) . lift $
        cache sender . mcWaiting %= Seq.drop 1
      -- Put message in retransmission cache, if configured
      lift $ cacheDelivered (sender,msg)
      -- Pass payload to application
      id %= (|> (msg^.msgPayload))
      return $ Just (msgId sender msg)
    ClockRejected mid -> lift $ do
      when fresh $ do
        ccmStats . totalOutOfOrder += 1
        -- Put message into waiting queue.  We know that this message
        -- goes at the end of the waiting queue, because we only allow
        -- messages into the waiting queue in sender-order.
        cache sender . mcWaiting %= (|> msg)

      -- Set retry-trigger for the message ID that we are waiting for
      deferMsg sender mid

      -- Assemble error message details
      return Nothing
    ClockAlreadyAccepted -> do
      -- If the message was deferred, drop it from the waiting queue.
      when (not fresh) . lift $
        cache sender . mcWaiting %= Seq.drop 1

      return Nothing
    ClockSO -> do
      -- In this case, it's not possible for the message to have been
      -- previously deferred, so we don't need to clean anything up.
      return Nothing

{- | This is like the 'Data.Maybe.catMaybes' function, but runs over
   sequences instead of lists. -}
justSeq :: Seq (Maybe a) -> Seq a
justSeq Seq.Empty = Seq.Empty
justSeq (Just a Seq.:<| s) = a Seq.:<| justSeq s
justSeq (Nothing Seq.:<| s) = justSeq s
