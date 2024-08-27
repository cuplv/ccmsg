{-# LANGUAGE LambdaCase #-}

module Network.Ccm.Algorithm
  ( inputPost
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

{- | Input a newly-received post, returning any output post payloads
   that result.

   Posts are supposed to be input in sender-order. Repeat posts will
   be silently dropped, and skipped posts will generate an error.
-}
inputPost
  :: (Monad m)
  => (NodeId, AppMsg)
  -> CcmST m (Seq ByteString)
inputPost (sender,msg) =
  punchInputClock sender (msg^.msgClock) >>= \case
    ICInput ->
      -- If 'ICInput' is returned, it means that the inputClock has
      -- been modified to include the new post.
      (flip execStateT) Seq.Empty $ do
        tryAcceptFresh sender msg >>= \case
          Just mid -> acceptRec mid
          Nothing -> return ()
    ICRepeat -> return Seq.Empty
    ICSkipped sn -> error $
      "Got post "
      ++ show (nextNum sender (msg^.msgClock))
      ++ " from node "
      ++ show sender
      ++ " before post "
      ++ show sn

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

  lift (punchOutputClock sender (msg^.msgClock)) >>= \case
    OCOutput -> do
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
    OCDefer mid -> lift $ do
      when fresh $ do
        ccmStats . totalOutOfOrder += 1
        -- Put message into waiting queue.  We know that this message
        -- goes at the end of the waiting queue, because we only allow
        -- messages into the waiting queue in sender-order.
        cache sender . mcWaiting %= (|> msg)

      -- Set retry-trigger for the message ID that we are waiting for
      deferMsg sender mid

      return Nothing

{- | This is like the 'Data.Maybe.catMaybes' function, but runs over
   sequences instead of lists. -}
justSeq :: Seq (Maybe a) -> Seq a
justSeq Seq.Empty = Seq.Empty
justSeq (Just a Seq.:<| s) = a Seq.:<| justSeq s
justSeq (Nothing Seq.:<| s) = justSeq s
