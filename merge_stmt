merge into MTA_RIDERSHIP.RAW.MTA_RIDERSHIP_TGT tgt
using snow_stream_stock src
on TGT.symbol = src.symbol 
and TGT.timestamp = src.timestamp
when matched then update
set OPEN = src.open
when not matched then insert (
SYMBOL,
SERIES,
OPEN,
HIGH,
LOW,
CLOSE,
LAST,
PREVCLOSE,
TOTTRDQTY,
TOTTRDVAL,
TIMESTAMP,
TOTALTRADES,
ISIN )
values (src.SYMBOL,
src.SERIES,
src.OPEN,
src.HIGH,
src.LOW,
src.CLOSE,
src.LAST,
src.PREVCLOSE,
src.TOTTRDQTY,
src.TOTTRDVAL,
src.TIMESTAMP,
src.TOTALTRADES,
src.ISIN );
