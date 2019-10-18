%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Carlos Gonzalez Florido.  All Rights Reserved.
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

%% @doc Main functions
-module(nkpubsub).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([publish/1, subscribe/1, subscribe/2, unsubscribe/1, unsubscribe/2]).
-export([publish_one/1, parse/1]).

-include("nkpubsub.hrl").


%% ===================================================================
%% Types
%% ===================================================================


-type event() :: #nkpubsub{} | event_data().

-type event_data() ::
    #{
        topic := topic(),
        app => app(),
        class => class(),
        type => type(),
        obj_id => obj_id(),
        namespace => namespace(),
        body => body()
    }.

-type topic() :: atom() | binary().
-type app() :: atom() | binary().
-type class() :: atom() | binary().
-type type() :: atom() | binary().
-type obj_id() :: atom() | binary().
-type namespace() :: atom() | binary().
-type body() :: map().



%% ===================================================================
%% Public
%% ===================================================================

%% @doc Sends an event
%% If will send the event to all subscribers:
%% - Registered with the same Topic, Class and Type
%% - Registered with same Topic and Class, and empty Type
%% - Registered with same Topic and Type, and empty Class
%% - Registered with same Topic, empty Class and Type
%%
%% If obj_id is included in subscription, only those processes are notified
%% If app is included, only those processes are notified
%% If namespace is included, is used as a prefix
%% If a body is included, it will be merged with any registered process one
%%
%% Subscribers will receive an event {nkpubsub, Event}
-spec publish(event()) ->
    ok | {error, term()}.

publish(Event) ->
    case parse(Event) of
        {ok, Event2} ->
            lists:foreach(
                fun(ServerPid) ->
                    gen_server:cast(ServerPid, {publish, Event2})
                end,
                nkpubsub_srv:find_all_servers(Event2));
        {error, Error} ->
            {error, Error}
    end.


%% @doc Similar to publish/1, but will select one single subscriber
-spec publish_one(event()) ->
    ok | not_found.

publish_one(Event) ->
    case parse(Event) of
        {ok, Event2} ->
            Pids = nkpubsub_srv:find_all_servers(Event2),
            do_publish_one(Pids, Event2);
        {error, Error} ->
            {error, Error}
    end.


%% @private
do_publish_one([], _Event) ->
    not_found;

do_publish_one([Pid|Rest], Event) ->
    case gen_server:call(Pid, {publish_one, Event}) of
        ok ->
            ok;
        _ ->
            do_publish_one(Rest, Event)
    end.


%% @doc Register a process to receive events
%% Empty fields (subclass, type, obj_id, namespace) means match any
%% You SHOULD monitor the pid() and re-register if it fails
-spec subscribe(event()) ->
    {ok, [pid()]} | {error, term()}.

subscribe(Event) ->
    subscribe(Event, self()).


-spec subscribe(event(), pid()) ->
    {ok, [pid()]} | {error, term()}.

subscribe(Event, Pid) ->
    case parse(Event) of
        {ok, Event2} ->
            case do_subscribe_find(Event2, 50) of
                {ok, ServerPid} ->
                    % Caller should monitor ServerPid
                    gen_server:cast(ServerPid, {subscribe, Event2, Pid}),
                    {ok, ServerPid};
                error ->
                    {error, nkpubsub_registration_error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @private
do_subscribe_find(Event, Count) when Count > 0 ->
    case nkpubsub_srv:find_server(Event) of
        {ok, Pid} ->
            {ok, Pid};
        not_found ->
            case nkpubsub_srv:start_server(Event) of
                {ok, Pid} ->
                    {ok, Pid};
                {error, Error} ->
                    lager:notice("NkPUBSUB retrying registration: ~p", [Error]),
                    timer:sleep(100),
                    do_subscribe_find(Event, Count-1)
            end
    end;

do_subscribe_find(Event, _Count) ->
    lager:warning("NkPUBSUB could not register ~p", [lager:pr(Event, ?MODULE)]),
    error.


%% @doc
-spec unsubscribe(event()) ->
    ok.

unsubscribe(Event) ->
    unsubscribe(Event, self()).


%% @doc
-spec unsubscribe(event(), pid()) ->
    ok.

unsubscribe(Event, Pid) ->
    case parse(Event) of
        {ok, Event2} ->
            case nkpubsub_srv:find_server(Event2) of
                {ok, ServerPid} ->
                    gen_server:cast(ServerPid, {unsubscribe, Event2, Pid});
                not_found ->
                    ok
            end;
        {error, Error} ->
            {error, Error}
    end.


%% ===================================================================
%% Internal
%% ===================================================================


%% @doc Tries to parse a event-type object
-spec parse(nkpubsub:event()) ->
    {ok, #nkpubsub{}} | {error, term()}.

parse(#nkpubsub{}=Event) ->
    {ok, Event};

parse(Data) ->
    Syntax = #{
        topic => binary,
        app => binary,
        class => binary,
        type => binary,
        obj_id => binary,
        namespace => binary,
        body => map,
        '__defaults' => #{
            app => <<>>,
            class => <<>>,
            type => <<>>,
            obj_id => <<>>,
            namespace => <<>>
        },
        '__mandatory' => [topic]
    },
    case nklib_syntax:parse(Data, Syntax) of
        {ok, Parsed, []} ->
            #{
                topic := Topic,
                app := App,
                class := Class,
                type := Type,
                obj_id := ObjId,
                namespace := Namespace
            } = Parsed,
            Event = #nkpubsub{
                topic = Topic,
                app = App,
                class = Class,
                type = Type,
                obj_id = ObjId,
                namespace = Namespace,
                body = maps:get(body, Parsed, #{})
            },
            {ok, Event};
        {ok, _, _, [Field|_]} ->
            {error, {field_unknown, Field}};
        {error, {syntax_error, Error}} ->
            {error, {syntax_error, Error}};
        {error, {missing_mandatory_field, Field}} ->
            {error, {field_missing, Field}};
        {error, Error} ->
            {error, Error}
    end.


%%%% @doc Serializes an event
%%-spec unparse(#nkpubsub{}) ->
%%    map().
%%
%%unparse(Event) ->
%%    #nkpubsub{
%%        app = _SrvId,
%%        class = Class,
%%        subclass = Sub,
%%        type = Type,
%%        obj_id = ObjId,
%%        body = Body
%%    } = Event,
%%    Base = [
%%        {class, Class},
%%        case to_bin(Sub) of
%%            <<>> -> [];
%%            Sub2 -> {subclass, Sub2}
%%        end,
%%        case to_bin(Type) of
%%            <<>> -> [];
%%            Type2 -> {type, Type2}
%%        end,
%%        case to_bin(ObjId) of
%%            <<>> -> [];
%%            ObjId2 -> {obj_id, ObjId2}
%%        end,
%%        case is_map(Body) andalso map_size(Body) > 0 of
%%            true -> {body, Body};
%%            _ -> []
%%        end
%%    ],
%%    maps:from_list(lists:flatten(Base)).
%%
%%
%%%% @doc Serializes an event
%%-spec unparse2(#nkpubsub{}) ->
%%    map().
%%
%%unparse2(Event) ->
%%    #nkpubsub{
%%        app = _SrvId,
%%        class = Class,
%%        subclass = Sub,
%%        type = Type,
%%        obj_id = ObjId,
%%        namespace = Namespace,
%%        body = Body
%%    } = Event,
%%    Ev = case {Sub, Type} of
%%        {<<>>, <<>>} -> Class;
%%        {_, <<>>} -> <<Class/binary, $/, Sub/binary>>;
%%        {<<>>, _} -> <<Class/binary, "/*/", Type/binary>>;
%%        {_, _} -> <<Class/binary, $/, Sub/binary, $/, Type/binary>>
%%    end,
%%    Data1 = case is_map(Body) of
%%        true -> Body;
%%        _ -> #{}
%%    end,
%%    Data2 = case ObjId of
%%        <<>> -> Data1;
%%        _ -> Data1#{obj_id=>ObjId}
%%    end,
%%    Data3 = case Namespace of
%%        <<>> -> Data2;
%%        _ -> Data2#{namespace=>Namespace}
%%    end,
%%    #{
%%        event => Ev,
%%        data => Data3
%%    }.




%%%% @private
%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).





