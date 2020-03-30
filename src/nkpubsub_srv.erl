%% -------------------------------------------------------------------
%%
%% Copyright (c) 2020 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Registration server
-module(nkpubsub_srv).
-behaviour(gen_server).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([find_server/1, find_all_servers/1, start_server/1, set_stop_after_last/2]).
-export([start_link/3, get_all/0, dump/3]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).


-include("nkpubsub.hrl").
-define(LLOG(Type, Txt, Args, State),
    lager:Type("NkPUBSUB '~s/~s/~s' "++Txt,
        [State#state.topic, State#state.class, State#state.type|Args])).


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Finds the pid of a server for {Topic, Class, Type}, if registered
-spec find_server(#nkpubsub{}) ->
    {ok, pid()} | not_found.

find_server(#nkpubsub{topic=Topic, class=Class, type=Type}) ->
    do_find_server(Topic, Class, Type).


%% @private
do_find_server(Topic, Class, Type) ->
    case global:whereis_name({?MODULE, {Topic, Class, Type}}) of
        Pid when is_pid(Pid) ->
            {ok, Pid};
        undefined ->
            not_found
    end.


%% @doc
-spec find_all_servers(#nkpubsub{}) ->
    [pid()].

find_all_servers(#nkpubsub{topic=Topic, class=Class, type=Type}) ->
    List = case {Class, Type} of
        {<<>>, <<>>} ->
            [{Topic, <<>>, <<>>}];
        {Class, <<>>} ->
            [{Topic, Class, <<>>}, {Topic, <<>>, <<>>}];
        _ ->
            [{Topic, Class, Type}, {Topic, Class, <<>>}, {Topic, <<>>, Type}, {Topic, <<>>, <<>>}]
    end,
    find_add_servers(List, []).


%% @private
find_add_servers([], Acc) ->
    Acc;

find_add_servers([{Topic, Class, Type}|Rest], Acc) ->
    Acc2 = case do_find_server(Topic, Class, Type) of
        {ok, Pid} ->
            [Pid|Acc];
        not_found ->
            Acc
    end,
    find_add_servers(Rest, Acc2).


%% @doc
-spec start_server(#nkpubsub{}) ->
    {ok, pid()} | {error, term()}.

start_server(Event) ->
    Nodes = nklib_util:randomize([node()|nodes()]),
    start_server(Nodes, Event).


%% @private
start_server([], _Event) ->
    {error, could_not_start_server};

start_server([Node|Rest], #nkpubsub{}=Event) ->
    case rpc:call(Node, nkpubsub_sup, start_event_server, [Event]) of
        {ok, Pid} ->
            lager:notice("NkPUBSUB server for ~p started at ~p (~p)",
                [lager:pr(Event, ?MODULE), node(Pid), Pid]),
            {ok, Pid};
        Other ->
            lager:info("NkPUBSUB could not start server for ~p at ~p: ~p",
                [lager:pr(Event, ?MODULE), Node, Other]),
            start_server(Rest)
    end.


%% @private Sets a server to stop after las registration leaves
set_stop_after_last(Event, Bool) when is_boolean(Bool) ->
    case find_server(Event) of
        {ok, Pid} ->
            gen_server:cast(Pid, {stop_after_last, Bool});
        not_found ->
            ok
    end.


%% @private
get_all() ->
    [
        {Spec, global:whereis_name({?MODULE, Spec})}
        || {?MODULE, Spec} <- global:registered_names()
    ].


%% ===================================================================
%% gen_server
%% ===================================================================

% This server is already for {Topic, Class, Type}, son only App and ObjId are left
% to identify the subscription
-type key() :: {nkpubsub:app(), nkpubsub:obj_id()}.

-record(state, {
    topic :: nkpubsub:topic(),
    class :: nkpubsub:class(),
    type :: nkpubsub:type(),
    subs = #{} :: #{key() => [{pid(), nkpubsub:namespace(), nkpubsub:body()}]},
    pids = #{} :: #{pid() => {Mon::reference(), [key()]}},
    stop_after_last = false :: boolean()
}).


%% @private
start_link(Class, Sub, Type) ->
    gen_server:start_link(?MODULE, {Class, Sub, Type}, []).


%% @private
-spec init(term()) ->
    {ok, #state{}}.

init({Topic, Class, Type}) ->
    RegId = {?MODULE, {Topic, Class, Type}},
    case global:register_name(RegId, self(), fun global:random_notify_name/3) of
        yes ->
            nklib_proc:put(?MODULE, {Topic, Class, Type}),
            State = #state{topic=Topic, class=Class, type=Type},
            ?LLOG(debug, "starting server (~p)", [self()], State),
            {ok, State};
        no ->
            ignore
    end.


%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}} | {noreply, #state{}} | {stop, normal, ok, #state{}}.

handle_call({publish_one, Event}, _From, State) ->
    #nkpubsub{class=Class, app=App, obj_id=ObjId, debug=Debug} = Event,
    #state{class=Class, subs=Subs} = State,
    PidTerms = case maps:get({App, ObjId}, Subs, []) of
        [] ->
            case maps:get({App, <<>>}, Subs, []) of
                [] ->
                    case maps:get({<<>>, ObjId}, Subs, []) of
                        [] ->
                            maps:get({<<>>, <<>>}, Subs, []);
                        List ->
                            List
                    end;
                List ->
                    List
            end;
        List ->
            List
    end,
    case Debug of
        true ->
            ?LLOG(debug, "call ~s:~s: ~p", [App, ObjId, PidTerms], State);
        _ ->
            ok
    end,
    case PidTerms of
        [] ->
            {reply, not_found, State};
        _ ->
            Pos = nklib_util:l_timestamp() rem length(PidTerms) + 1,
            {Pid, Dom, RegBody} = lists:nth(Pos, PidTerms),
            send_events([{Pid, Dom, RegBody}], Event, [], State),
            {reply, ok, State}
    end;

handle_call(dump, _From, #state{subs=Subs, pids=Pids}=State) ->
    {reply, {Subs, map_size(Subs), Pids, map_size(Pids)}, State};

handle_call(Msg, _From, State) ->
    lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_cast(term(), #state{}) ->
    {noreply, #state{}}.

handle_cast({publish, Event}, State) ->
    #state{class=Class, subs=Subs} = State,
    #nkpubsub{class=Class, app=App, obj_id=ObjId, debug=Debug} = Event,
    PidTerms1 = maps:get({App, ObjId}, Subs, []),
    PidTerms2 = maps:get({App, <<>>}, Subs, []),
    PidTerms3 = case App of
        <<>> ->
            [];
        _ ->
            maps:get({<<>>, ObjId}, Subs, [])
    end,
    PidTerms4 = case App of
        <<>> ->
            [];
        _ ->
            maps:get({<<>>, <<>>}, Subs, [])
    end,
    case Debug of
        true ->
            ?LLOG(info, "send ~s:~s ~p,~p,~p,~p",
                [App, ObjId, PidTerms1, PidTerms2, PidTerms3, PidTerms4], State);
        _ ->
            ok
    end,
    Acc1 = send_events(PidTerms1, Event, [], State),
    Acc2 = send_events(PidTerms2, Event, Acc1, State),
    Acc3 = send_events(PidTerms3, Event, Acc2, State),
    _Acc4 = send_events(PidTerms4, Event, Acc3, State),
    {noreply, State};

handle_cast({subscribe, Event, ProcPid}, State) ->
    case Event of
        #nkpubsub{app=App, obj_id=ObjId, debug=true} ->
            ?LLOG(debug, "registered ~s:~s (~p)", [App, ObjId, ProcPid], State);
        _ ->
            ok
    end,
    State2 = do_subscribe(Event, ProcPid, State),
    {noreply, State2};

handle_cast({unsubscribe, Event, ProcPid}, State) ->
    #nkpubsub{app=App, obj_id=ObjId} = Event,
    case Event of
        #nkpubsub{debug=true} ->
            ?LLOG(debug, "unregistered ~s:~s (~p)", [App, ObjId, ProcPid], State);
        _ ->
            ok
    end,
    State2 = do_unsubscribe([{App, ObjId}], ProcPid, State),
    check_stop(State2);

handle_cast({stop_after_last, Bool}, State) ->
    State2 = State#state{stop_after_last = Bool},
    check_stop(State2);

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(Msg, State) ->
    lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
    {noreply, #state{}}.

handle_info({'DOWN', Mon, process, Pid, _Reason}, #state{pids=Pids}=State) ->
    case maps:find(Pid, Pids) of
        {ok, {Mon, Keys}} ->
            %% lists:foreach(
            %%    fun({App, ObjId}) ->
            %%        ?LLOG(info, "unregistered ~s:~s (down)", [App, ObjId], State)
            %%    end,
            %%    Keys),
            State2 = do_unsubscribe(Keys, Pid, State),
            check_stop(State2);
        error ->
            lager:warning("Module ~p received unexpected down: ~p", [?MODULE, Pid]),
            {noreply, State}
    end;

handle_info({global_name, conflict, _Name}, State) ->
    lager:notice("NkPUBSUB server stopping because of conflict"),
    {stop, normal, State};

handle_info(Info, State) ->
    lager:warning("Module ~p received unexpected info ~p", [?MODULE, Info]),
    {noreply, State}.


%% @private
-spec code_change(term(), #state{}, term()) ->
    {ok, #state{}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
-spec terminate(term(), #state{}) ->
    ok.

terminate(_Reason, State) ->
    ?LLOG(info, "stopping server (~p)", [self()], State),
    ok.



%% ===================================================================
%% Private
%% ===================================================================


%% @private
do_subscribe(Event, Pid, #state{subs=Subs, pids=Pids}=State) ->
    #nkpubsub{app=App, obj_id=ObjId, namespace=Namespace, body=Body} = Event,
    Key = {App, ObjId},
    PidBodyList2 = case maps:find(Key, Subs) of
        {ok, PidBodyList} ->   % [{pid(), namespace(), body)}]
            lists:keystore(Pid, 1, PidBodyList, {Pid, Namespace, Body});
        error ->
            [{Pid, Namespace, Body}]
    end,
    Subs2 = maps:put(Key, PidBodyList2, Subs),
    {Mon2, Keys2} = case maps:find(Pid, Pids) of
        {ok, {Mon, Keys}} ->    % [reference(), [key()]]
            {Mon, nklib_util:store_value(Key, Keys)};
        error ->
            Mon = monitor(process, Pid),
            {Mon, [Key]}
    end,
    Pids2 = maps:put(Pid, {Mon2, Keys2}, Pids),
    State#state{subs=Subs2, pids=Pids2}.


%% @private
do_unsubscribe([], _Pid, State) ->
    State;

do_unsubscribe([Key|Rest], Pid, #state{subs=Subs, pids=Pids}=State) ->
    Subs2 = case maps:find(Key, Subs) of
        {ok, PidBodyList} ->
            case lists:keydelete(Pid, 1, PidBodyList) of
                [] ->
                    maps:remove(Key, Subs);
                PidBodyList2 ->
                    maps:put(Key, PidBodyList2, Subs)
            end;
        error ->
            Subs
    end,
    Pids2 = case maps:find(Pid, Pids) of
        {ok, {Mon, Keys}} ->
            case Keys -- [Key] of
                [] ->
                    demonitor(Mon),
                    maps:remove(Pid, Pids);
                Keys2 ->
                    maps:put(Pid, {Mon, Keys2}, Pids)
            end;
        error ->
            Pids
    end,
    do_unsubscribe(Rest, Pid, State#state{subs=Subs2, pids=Pids2}).


%% @private
send_events([], _Event, Acc, _State) ->
    Acc;

send_events([{Pid, RegNamespace, RegBody}|Rest], Event, Acc, State) ->
    case lists:member(Pid, Acc) of
        false ->
            #nkpubsub{namespace=Namespace, body=Body, debug=Debug}=Event,
            %% We are subscribed to any namespace or the namespace of the event
            %% is longer than the registered namespace
            case check_namespace(RegNamespace, Namespace) of
                true ->
                    Event2 = Event#nkpubsub{body=maps:merge(RegBody, Body)},
                    case Debug of
                        true  ->
                            ?LLOG(info,
                                "sending event ~p to ~p", [lager:pr(Event2, ?MODULE), Pid], State);
                        _ ->
                            ok
                    end,
                    Pid ! {nkpubsub, Event2},
                    send_events(Rest, Event, [Pid|Acc], State);
                false ->
                    send_events(Rest, Event, Acc, State)
            end;
        true ->
            send_events(Rest, Event, Acc, State)
    end.


%% @private
check_namespace(<<>>, _) ->
    true;
check_namespace(Reg, Namespace) ->
    Size = byte_size(Reg),
    case Namespace of
        <<Reg:Size/binary, _/binary>> -> true;
        _ -> false
    end.


%% @private
check_stop(#state{pids=Pids, stop_after_last=Stop}=State) ->
    case Stop andalso map_size(Pids)==0 of
        true ->
            {stop, normal, State};
        false ->
            {noreply, State}
    end.


%% @private
dump(Class, Sub, Type) ->
    Class2 = to_bin(Class),
    Sub2 = to_bin(Sub),
    Type2 = to_bin(Type),
    {ok, Pid} = do_find_server(Class2, Sub2, Type2),
    gen_server:call(Pid, dump).


%% @private
to_bin(Term) -> nklib_util:to_binary(Term).



%% ===================================================================
%% Tests
%% ===================================================================


%-define(TEST, 1).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%-compile([export_all, nowarn_export_all


basic_test_() ->
    {setup,
        fun() ->
            ?debugFmt("Starting ~p", [?MODULE]),
            test_reset()
        end,
        fun(_Stop) ->
            test_reset()
        end,
        [
            fun test1/0,
            fun test2/0,
            fun test3/0,
            fun test4/0
        ]
    }.


test_reset() ->
    Ev = #nkpubsub{topic = <<"t">>, class = <<"c">>, type = <<"y">>},
    case find_server(Ev) of
        {ok, Pid} ->
            gen_server:cast(Pid, stop),
            timer:sleep(50);
        not_found ->
            ok
    end.



test1() ->
    Reg = #{topic=>t, app=>srv, class=>c, type=>y},
    Self = self(),
    nkpubsub:subscribe(Reg#{obj_id=>id1, body=>#{b1=>1}}),
    nkpubsub:subscribe(Reg#{obj_id=>id2, body=>#{b2=>2}}),
    {
        #{
            {<<"srv">>, <<"id1">>} := [{Self, <<>>, #{b1:=1}}],
            {<<"srv">>, <<"id2">>} := [{Self, <<>>, #{b2:=2}}]
        },
        2,
        #{
            Self := {_, [{<<"srv">>, <<"id2">>}, {<<"srv">>, <<"id1">>}]}
        },
        1
    } =
        dump(t, c, y),

    nkpubsub:unsubscribe(Reg#{obj_id=>id1}),
    {
        #{{<<"srv">>, <<"id2">>} := [{Self, <<>>, #{b2:=2}}]},
        1,
        #{Self := {_, [{<<"srv">>, <<"id2">>}]}},
        1
    } =
        dump(t, c, y),

    nkpubsub:unsubscribe(Reg#{obj_id=>id3}),
    nkpubsub:unsubscribe(Reg#{obj_id=>id2}),
    {_, 0, _, 0} = dump(t, c, y),
    ok.


test2() ->
    P1 = test_reg(1, b1),
    P2 = test_reg(1, b1),
    P3 = test_reg(1, b1b),
    P4 = test_reg(2, b2),
    P5 = test_reg(3, b3),
    P6 = test_reg(3, b3b),
    P7 = test_reg(<<>>, b7),
    timer:sleep(50),

    Reg = #{topic=>t, class=>c, type=>y, app=>srv},
    lists:foreach(
        fun(_) ->
            nkpubsub:publish_one(Reg#{obj_id=>0}),
            receive
                {c, _RP1, <<"0">>, RB1} ->
                    true = lists:member(RB1, [b1, b1b, b2, b3, b3b, b7]);
                O ->
                    error(O)
            after 100 ->
                error(?LINE)

            end
        end,
        lists:seq(1, 100)),

    lists:foreach(
        fun(_) ->
            nkpubsub:publish_one(Reg#{obj_id=>1}),
            receive
                {c, _RP2, <<"1">>, RB2} -> true = lists:member(RB2, [b1, b1b, b7])
            after 100 -> error(?LINE)
            end
        end,
        lists:seq(1, 100)),

    lists:foreach(
        fun(_) ->
            nkpubsub:publish_one(Reg#{obj_id=>2}),
            receive
                {c, RP3, <<"2">>, RB2} ->
                    true = lists:member(RB2, [b2, b7]),
                    true = lists:member(RP3, [P4, P7])
            after 100 ->
                error(?LINE)
            end
        end,
        lists:seq(1, 100)),

    lists:foreach(
        fun(_) ->
            nkpubsub:publish_one(Reg#{obj_id=>3}),
            receive
                {c, _RP3, <<"3">>, RB2} -> true = lists:member(RB2, [b3, b3b, b7])
            after 100 -> error(?LINE)
            end
        end,
        lists:seq(1, 100)),

    lists:foreach(
        fun(_) ->
            nkpubsub:publish_one(Reg#{obj_id=>25}),
            receive
                {c, P7, <<"25">>, b7} -> ok
            after 100 -> error(?LINE)
            end
        end,
        lists:seq(1, 100)),

    nkpubsub:publish(Reg#{obj_id=>0}),
    receive {c, P1, <<"0">>, b1} -> ok after 100 -> error(?LINE) end,
    receive {c, P2, <<"0">>, b1} -> ok after 100 -> error(?LINE) end,
    receive {c, P3, <<"0">>, b1b} -> ok after 100 -> error(?LINE) end,
    receive {c, P4, <<"0">>, b2} -> ok after 100 -> error(?LINE) end,
    receive {c, P5, <<"0">>, b3} -> ok after 100 -> error(?LINE) end,
    receive {c, P6, <<"0">>, b3b} -> ok after 100 -> error(?LINE) end,
    receive {c, P7, <<"0">>, b7} -> ok after 100 -> error(?LINE) end,

    nkpubsub:publish(Reg#{obj_id=>1}),
    receive {c, P1, <<"1">>, b1} -> ok after 100 -> error(?LINE) end,
    receive {c, P2, <<"1">>, b1} -> ok after 100 -> error(?LINE) end,
    receive {c, P3, <<"1">>, b1b} -> ok after 100 -> error(?LINE) end,
    receive {c, P7, <<"1">>, b7} -> ok after 100 -> error(?LINE) end,

    nkpubsub:publish(Reg#{obj_id=>2}),
    receive {c, P4, <<"2">>, b2} -> ok after 100 -> error(?LINE) end,
    receive {c, P7, <<"2">>, b7} -> ok after 100 -> error(?LINE) end,

    nkpubsub:publish(Reg#{obj_id=>3}),
    receive {c, P5, <<"3">>, b3} -> ok after 100 -> error(?LINE) end,
    receive {c, P6, <<"3">>, b3b} -> ok after 100 -> error(?LINE) end,
    receive {c, P7, <<"3">>, b7} -> ok after 100 -> error(?LINE) end,

    nkpubsub:publish(Reg#{obj_id=>33}),
    receive {c, P7, <<"33">>, b7} -> ok after 100 -> error(?LINE) end,

    receive _ -> error(?LINE) after 100 -> ok end,

    [P ! stop || P <- [P1, P2, P3, P4, P5, P6, P7]],
    timer:sleep(50),
    {_, 0, _, 0} = dump(t, c, y),
    ok.


test3() ->
    P1 = test_reg(1, b1),
    P2 = test_reg(1, b1),
    P3 = test_reg(1, b1b),
    P4 = test_reg(2, b2),
    P5 = test_reg(3, b3),
    P6 = test_reg(3, b3b),
    P7 = test_reg(<<>>, b7),
    timer:sleep(100),

    {
        #{
            {<<"srv">>, <<>>} := [{P7, <<>>, #{b7:=1}}],
            {<<"srv">>, <<"0">>} := L1,
            {<<"srv">>, <<"1">>} := L2,
            {<<"srv">>, <<"2">>} := [{P4, <<>>, #{b2:=1}}],
            {<<"srv">>, <<"3">>} := L3
        },
        5,
        #{
            P1 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<"1">>}]},
            P2 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<"1">>}]},
            P3 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<"1">>}]},
            P4 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<"2">>}]},
            P5 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<"3">>}]},
            P6 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<"3">>}]},
            P7 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<>>}]}
        },
        7
    }
        = dump(t, c, y),

    true = lists:sort(L1) == lists:sort(
        [{P1, <<>>, #{b1=>1}}, {P2, <<>>, #{b1=>1}}, {P3, <<>>, #{b1b=>1}}, {P4, <<>>, #{b2=>1}},
        {P5, <<>>, #{b3=>1}}, {P6, <<>>, #{b3b=>1}}, {P7, <<>>, #{b7=>1}}]),

    true = lists:sort(L2) == lists:sort(
        [{P1, <<>>, #{b1=>1}}, {P2, <<>>, #{b1=>1}}, {P3, <<>>, #{b1b=>1}}]),

    true = lists:sort(L3) == lists:sort([{P5, <<>>, #{b3=>1}}, {P6, <<>>   , #{b3b=>1}}]),

    P1 ! stop,
    P4 ! unreg,
    timer:sleep(100),
    {
        #{
            {<<"srv">>, <<>>} := [{P7, <<>>, #{b7:=1}}],
            {<<"srv">>, <<"0">>} := L4,
            {<<"srv">>, <<"1">>} := L5,
            {<<"srv">>, <<"3">>} := L6
        },
        4,
        #{
            P2 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<"1">>}]},
            P3 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<"1">>}]},
            P4 := {_, [{<<"srv">>, <<"0">>}]},
            P5 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<"3">>}]},
            P6 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<"3">>}]},
            P7 := {_, [{<<"srv">>, <<"0">>}, {<<"srv">>, <<>>}]}
        },
        6
    } =
        dump(t, c, y),

    true = lists:sort(L4) == lists:sort([
            {P2, <<>>, #{b1=>1}},
            {P3, <<>>, #{b1b=>1}},
            {P4, <<>>, #{b2=>1}},
            {P5, <<>>, #{b3=>1}},
            {P6, <<>>, #{b3b=>1}},
            {P7, <<>>, #{b7=>1}}
    ]),

    true = lists:sort(L5) == lists:sort([{P2, <<>>, #{b1=>1}}, {P3, <<>>, #{b1b=>1}}]),

    true = lists:sort(L6) == lists:sort([{P5, <<>>, #{b3=>1}}, {P6, <<>>, #{b3b=>1}}]),

    [P ! stop || P <- [P1, P2, P3, P4, P5, P6, P7]],
    ok.


test4() ->
    P1 = test_reg(4, <<"/a/b">>, b5),
    timer:sleep(50),
    Reg = #{topic=>t, class=>c, type=>y, app=>srv},
    nkpubsub:publish(Reg#{obj_id=>4, namespace => <<"/a/b">>}),
    nkpubsub:publish(Reg#{obj_id=>4, namespace => <<"/a/b/c">>}),
    nkpubsub:publish(Reg#{obj_id=>4, namespace => <<"/a/">>}),
    receive {c, P1, <<"4">>, b5} -> ok after 100 -> error(?LINE) end,
    receive {c, P1, <<"4">>, b5} -> ok after 100 -> error(?LINE) end,
    receive {c, P1, <<"4">>, b5} -> ok after 100 -> ok end,
    P1 ! stop,
    ok.



test_reg(I, B) ->
    test_reg(I, <<>>, B).


test_reg(I, Namespace, B) ->
    Reg = #{topic=>t, class=>c, type=>y, namespace=>Namespace, app=>srv},
    Self = self(),
    spawn(
        fun() ->
            nkpubsub:subscribe(Reg#{obj_id=>I, body=>#{B=>1}}),
            nkpubsub:subscribe(Reg#{obj_id=>0, body=>#{B=>1}}),
            test_reg_loop(Self, I)
        end).


test_reg_loop(Pid, I) ->
    receive
        {nkpubsub, Event} ->
            #nkpubsub{topic = <<"t">>, class= <<"c">>, type= <<"y">>,
                app = <<"srv">>, obj_id=Id, body=B} = Event,
            [{B1, 1}] = maps:to_list(B),
            Pid ! {c, self(), Id, B1},
            test_reg_loop(Pid, I);
        stop ->
            ok;
        unreg ->
            nkpubsub:unsubscribe(#{topic=>t, class=>c, type=>y, app=>srv, obj_id=>I}),
            test_reg_loop(Pid, I);
        _ ->
            error(?LINE)
    after 10000 ->
        ok
    end.


-endif.
