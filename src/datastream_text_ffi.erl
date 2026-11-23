%% Erlang FFI for the text functions in datastream.gleam.

-module(datastream_text_ffi).
-export([split_once/2, suffix/2]).

%% Splits at the first occurrence of Delimiter, comparing bytes. Because
%% UTF-8 is self-synchronising, a match always starts and ends on code
%% point boundaries. string:split/2 matches whole grapheme clusters
%% instead, so its result would depend on where chunks were cut.
split_once(Text, Delimiter) ->
    case binary:split(Text, Delimiter) of
        [Before, After] -> {ok, {Before, After}};
        [_] -> {error, nil}
    end.

%% Returns {Truncated, Suffix}: the last Size bytes of Text, extended
%% back to a code point boundary and past any regional indicators just
%% before it, so the suffix counts regional indicator pairs from the
%% same place as the whole text.
suffix(Text, Size) when byte_size(Text) =< Size ->
    {false, Text};
suffix(Text, Size) ->
    Start = back_over_indicators(Text, code_point_start(Text, byte_size(Text) - Size)),
    case Start of
        0 -> {false, Text};
        _ -> {true, binary:part(Text, Start, byte_size(Text) - Start)}
    end.

code_point_start(Text, Position) when Position > 0 ->
    case binary:at(Text, Position) of
        Byte when Byte >= 16#80, Byte =< 16#BF -> code_point_start(Text, Position - 1);
        _ -> Position
    end;
code_point_start(_, _) ->
    0.

%% A regional indicator is U+1F1E6..U+1F1FF, encoded F0 9F 87 A6..BF.
back_over_indicators(Text, Start) when Start >= 4 ->
    case binary:part(Text, Start - 4, 4) of
        <<16#F0, 16#9F, 16#87, Last>> when Last >= 16#A6, Last =< 16#BF ->
            back_over_indicators(Text, Start - 4);
        _ ->
            Start
    end;
back_over_indicators(_, Start) ->
    Start.
