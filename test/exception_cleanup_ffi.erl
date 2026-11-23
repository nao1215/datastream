-module(exception_cleanup_ffi).
-export([rescue/1, ledger_reset/0, ledger_open/0, ledger_close/1,
         ledger_report/0, note/1, notes/0]).

%% Runs Thunk and turns any exception into {error, nil}.
rescue(Thunk) ->
    try {ok, Thunk()}
    catch _:_ -> {error, nil}
    end.

%% A ledger counts close calls per opened resource in the process
%% dictionary of the test process.
ledger_reset() ->
    put(exception_cleanup_notes, []),
    put(exception_cleanup_ledger, #{}),
    put(exception_cleanup_next, 0),
    nil.

ledger_open() ->
    Id = get(exception_cleanup_next),
    put(exception_cleanup_next, Id + 1),
    put(exception_cleanup_ledger,
        maps:put(Id, 0, get(exception_cleanup_ledger))),
    Id.

ledger_close(Id) ->
    Ledger = get(exception_cleanup_ledger),
    put(exception_cleanup_ledger,
        maps:put(Id, maps:get(Id, Ledger, 100) + 1, Ledger)),
    nil.

%% {Opened, ResourcesWhoseCloseCountIsNotOne}
ledger_report() ->
    Ledger = get(exception_cleanup_ledger),
    {maps:size(Ledger),
     maps:size(maps:filter(fun(_, Closes) -> Closes =/= 1 end, Ledger))}.

note(Value) ->
    put(exception_cleanup_notes, [Value | get(exception_cleanup_notes)]),
    nil.

notes() -> lists:reverse(get(exception_cleanup_notes)).
