{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.State
  ( CcmState
  , CcmST
  , newCcmState
  , tryDeliver
  , recordSend
  , localClock
  , MsgId
  , AppMsg
  , SimpleAppMsg (..)
  , mkSimpleAppMsg
  , mkCausalAppMsg
  , msgClock
  , msgPayload
  , CausalError (..)
  , showCausalError'
  , ccmEnforceCausal
  ) where

import Network.Ccm.Bsm
import Network.Ccm.Lens
import Network.Ccm.Types
import Network.Ccm.VClock

import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Foldable (foldlM)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromJust)
import Data.Sequence (Seq,(<|),(|>))
import qualified Data.Sequence as Seq
import Data.Store
import Data.Store.TH
import Data.Traversable (for)
import GHC.Exts (toList,fromList)

type MsgId = (NodeId, SeqNum)

data AppMsg
  = AppMsg { _msgClock :: VClock
           , _msgPayload :: ByteString
           }
  deriving (Show,Eq)

makeStore ''AppMsg

makeLenses ''AppMsg

data SimpleAppMsg
  = SimpleAppMsg ByteString
  deriving (Show,Eq)

makeStore ''SimpleAppMsg

mkSimpleAppMsg :: ByteString -> SimpleAppMsg
mkSimpleAppMsg = SimpleAppMsg

mkCausalAppMsg :: VClock -> ByteString -> AppMsg
mkCausalAppMsg = AppMsg

data CausalError
  = CausalError
    { errorMsgSender :: NodeId
    , errorLocalClock :: VClock
    , errorMsgClock :: VClock
    }
  deriving (Eq,Ord)

instance Show CausalError where
  show e =
    "[!]Causal: "
    ++ show (errorMsgSender e)
    ++ " "
    ++ show (errorMsgClock e)
    ++ " => "
    ++ show (errorLocalClock e)

{-| Show causal error, including receiver's ID -}
showCausalError' :: NodeId -> CausalError -> String
showCausalError' n e =
  "[!]Causal: "
  ++ show (errorMsgSender e)
  ++ " "
  ++ show (errorMsgClock e)
  ++ " => "
  ++ show n
  ++ " "
  ++ show (errorLocalClock e)

{-| Sequence number of a message, in terms of its sender and its clock. -}
msgSeqNum :: NodeId -> AppMsg -> SeqNum
msgSeqNum sender msg = nextNum sender (msg ^. msgClock)

msgId :: NodeId -> AppMsg -> MsgId
msgId sender msg = (sender, nextNum sender (msg^.msgClock))

data CcmState
  = CcmState
    { _ccmCache :: Map NodeId (SeqNum, Seq AppMsg)
    , _ccmWaiting :: Map NodeId (SeqNum, Seq MsgId)
    , _ccmSeen :: Map NodeId VClock
    , _localClock :: VClock
    , _ccmMsgStore :: Map MsgId AppMsg
    , _ccmEnforceCausal :: Bool
    }

type CcmST m = StateT CcmState m

makeLenses ''CcmState

newCcmState :: Bool -> CcmState
newCcmState causal = CcmState
  { _ccmCache = Map.empty
  , _ccmWaiting = Map.empty
  , _ccmSeen = Map.empty
  , _localClock = zeroClock
  , _ccmMsgStore = Map.empty
  , _ccmEnforceCausal = causal
  }

{-| Try to deliver a message. If this fails, delivery will be retried
  later automatically. If it succeeds, the delivered message, and any
  deferred messages that were delivered as a result, are returned in
  causal order. -}
tryDeliver
  :: (Monad m)
  => (NodeId, AppMsg)
  -> CcmST m (Either CausalError (VClock, Seq ByteString))
tryDeliver (sender,msg) = do
  let m = msgId sender msg
  causal <- use $ ccmEnforceCausal
  r <- if causal
    then punchClock sender (msg^.msgClock)
    else return $ Right ()
  case r of
    Right () -> do
      -- Retry delivery of deferred msgs, collecting ids of successful
      -- retries.
      retries <- retryLoop m

      -- Get payloads of successfully retried msgs.
      retryPayloads <- for retries $ \m1 -> do
        -- Set m1's content to Nothing, returning the old value.
        c <- ccmMsgStore . at m1 <<.= Nothing
        case c of
          Just c -> return (c^.msgPayload)
          Nothing -> error $ "Deferred msg had no stored content " ++ show m

      local <- use localClock
      return $ Right (local, (msg^.msgPayload) <| retryPayloads)

    Left m2 -> do
      -- Store msg's content for retry (since it is being deferred for
      -- the first time).
      ccmMsgStore . at m .= Just msg
      -- Register msg's id to be retried later.
      deferMsgId m m2

      local <- use localClock
      return . Left $ CausalError
        { errorMsgSender = sender
        , errorMsgClock = msg^.msgClock
        , errorLocalClock = local
        }

{-| Run @deferMsgId m1 m2@ when you must delay the delivery of @m1@
  until after @m2@ has been delivered. -}
deferMsgId :: (Monad m) => MsgId -> MsgId -> CcmST m ()
deferMsgId m1 (i2,n2) =
  let
    f v = case v of
      Just (n',d) ->
        Just (min n2 n', d |> m1)
      Nothing ->
        Just (n2, fromList [m1])
  in
    ccmWaiting . at i2 %= f

{- | Try to deliver a msg that has preveously been deferred.  To call
   this, provide the 'MsgId'.  We assume that the corresponding
   content is in 'ccmMsgStore'.  If delivery is successful this time,
   return 'True'.

   If deliver is unsuccessful, the 'MsgId' is re-deferred and then
   'False' is returned. -}
retryDeliver :: (Monad m) => MsgId -> CcmST m Bool
retryDeliver m1 = do
  mdep <- use $ ccmMsgStore . at m1
  dep <- case mdep of
    Just msg -> return $ msg^.msgClock
    Nothing -> do
      msgStore <- use ccmMsgStore
      error $
        "No stored clock for MsgId "
        ++ show m1
        ++ ", "
        ++ show msgStore
  result <- punchClock (fst m1) dep
  case result of
    Right () -> do
      return True
    Left m2 -> do
      deferMsgId m1 m2
      return False

{- | @punchClock i v@ attempts to update the local clock to include a
   new message from process @i@, which has dependencies @v@.

   If this fails, the local clock is unchanged and a message-id that
   has been witnessed by @v@ but not witnessed by the local clock is
   returned. -}
punchClock :: (Monad m) => NodeId -> VClock -> CcmST m (Either MsgId ())
punchClock msgSender msgClock = do
  local <- use localClock
  case leVC' msgClock local of
    Right () -> do
      localClock %= tick msgSender
      return $ Right ()
    Left m ->
      return $ Left m

{-| Record the sending of a new message, trusting that the new message's
  clock is satisfied by the local clock.

  TODO: The message should be cached for later re-send requests.  For
  now, only the local clock is modified. -}
recordSend :: (Monad m) => (NodeId, AppMsg) -> CcmST m ()
recordSend (sender,_) = do
  localClock %= tick sender

{-| Cache a delivered message, so that it can be sent to other nodes
  that ask for it later. -}
cacheMsg :: (Monad m) => (NodeId, AppMsg) -> CcmST m ()
cacheMsg (sender,msg) = do
  ccmCache . at sender %=
    (\v -> case v of
        Nothing -> 
          let seqNum = nextNum sender (msg^.msgClock)
          in Just (seqNum, fromList [msg])
        Just (seqNum,d) ->
          Just (seqNum,d |> msg))

{-| Return any 'MsgId's that were waiting on the given 'MsgId', removing
  them from the internal waiting map. -}
revive :: (Monad m) => MsgId -> CcmST m (Seq MsgId)
revive (sender,sn) = do
  waiting <- use (ccmWaiting.at sender)
  case (waiting :: Maybe (SeqNum, Seq MsgId)) of
    Just (sn',msgs) | sn' <= sn -> do
      ccmWaiting.at sender .= Nothing
      return msgs
    _ -> return (fromList [])

{-| Retry delivery of any messages that depended on the given 'MsgId',
  and any messages that depended on those messages, and so on.  Return
  the newly-delivered 'MsgId's. -}
retryLoop :: (Monad m) => MsgId -> CcmST m (Seq MsgId)
retryLoop m = do
  let f ms m1 = do
        -- @retryDeliver m1@ automatically re-defers m1 when it fails.
        result <- retryDeliver m1
        if result
          then return $ ms |> m1
          else return $ ms
  -- Collect all 'MsgId's from any queue that was waiting on m.
  ms <- revive m
  -- Retry delivery of revived 'MsgId's, re-deferring failures and
  -- returning successes.
  msDlv <- foldlM f Seq.empty ms
  -- Loop on successfully retried 'MsgId's.
  loopMs <- traverse retryLoop msDlv
  return $ foldMap id (msDlv <| loopMs)
