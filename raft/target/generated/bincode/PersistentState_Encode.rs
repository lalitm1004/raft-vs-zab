impl :: bincode :: Encode for PersistentState
{
    fn encode < __E : :: bincode :: enc :: Encoder >
    (& self, encoder : & mut __E) ->core :: result :: Result < (), :: bincode
    :: error :: EncodeError >
    {
        :: bincode :: Encode :: encode(&self.current_term, encoder) ?; ::
        bincode :: Encode :: encode(&self.voted_for, encoder) ?; :: bincode ::
        Encode :: encode(&self.log, encoder) ?; core :: result :: Result ::
        Ok(())
    }
}