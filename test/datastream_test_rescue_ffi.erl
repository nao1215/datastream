-module(datastream_test_rescue_ffi).
-export([rescue/1]).

%% Runs `Thunk` and turns any exception into `{error, nil}`, so a test
%% can keep using the process after a panic.
rescue(Thunk) ->
    try {ok, Thunk()}
    catch _:_ -> {error, nil}
    end.
