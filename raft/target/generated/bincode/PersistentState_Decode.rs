impl < __Context > :: bincode :: Decode < __Context > for PersistentState
{
    fn decode < __D : :: bincode :: de :: Decoder < Context = __Context > >
    (decoder : & mut __D) ->core :: result :: Result < Self, :: bincode ::
    error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            current_term : :: bincode :: Decode :: decode(decoder) ?,
            voted_for : :: bincode :: Decode :: decode(decoder) ?, log : ::
            bincode :: Decode :: decode(decoder) ?,
        })
    }
} impl < '__de, __Context > :: bincode :: BorrowDecode < '__de, __Context >
for PersistentState
{
    fn borrow_decode < __D : :: bincode :: de :: BorrowDecoder < '__de,
    Context = __Context > > (decoder : & mut __D) ->core :: result :: Result <
    Self, :: bincode :: error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            current_term : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, voted_for : :: bincode :: BorrowDecode
            ::< '_, __Context >:: borrow_decode(decoder) ?, log : :: bincode
            :: BorrowDecode ::< '_, __Context >:: borrow_decode(decoder) ?,
        })
    }
}