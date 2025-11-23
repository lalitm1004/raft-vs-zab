impl < __Context > :: bincode :: Decode < __Context > for LogEntry
{
    fn decode < __D : :: bincode :: de :: Decoder < Context = __Context > >
    (decoder : & mut __D) ->core :: result :: Result < Self, :: bincode ::
    error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            term : :: bincode :: Decode :: decode(decoder) ?, command : ::
            bincode :: Decode :: decode(decoder) ?,
        })
    }
} impl < '__de, __Context > :: bincode :: BorrowDecode < '__de, __Context >
for LogEntry
{
    fn borrow_decode < __D : :: bincode :: de :: BorrowDecoder < '__de,
    Context = __Context > > (decoder : & mut __D) ->core :: result :: Result <
    Self, :: bincode :: error :: DecodeError >
    {
        core :: result :: Result ::
        Ok(Self
        {
            term : :: bincode :: BorrowDecode ::< '_, __Context >::
            borrow_decode(decoder) ?, command : :: bincode :: BorrowDecode ::<
            '_, __Context >:: borrow_decode(decoder) ?,
        })
    }
}