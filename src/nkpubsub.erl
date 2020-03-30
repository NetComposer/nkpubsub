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

%% @doc Main functions
%% - Any process can subscribe to events calling subscribe/1
%%      - For any combination of {topic, class, type} a process is started and
%%       registered globally (class and type are commonly <<>>, meaning 'any')
%%      - Subscriber must monitor the process pid, and re-suscribe if it fails
%%      - A new registration is inserted at server, maybe focusing on app and/or obj_id
%% - When a new event is generated, it is sent to the corresponding server based on
%%   topic, class and type
%%      - All subscribers are analyzed and found ones matching app and obj_uid, and
%%        notifications are sent
%%      - If the event has a namespace, only subscribers with a prefix are used
%%      - Body, if present, is merged with each registered
%%      - All selected receive a message {nkpubsub, Event}
%%
%% Efficiency
%% - Sending a event to a topic that does not exist it very efficient, since it will
%%   be detected locally that is not registered
%% - You should not create dynamically many susbscribers to different {topic, class, type},
%%   since they need to registered in all of the cluster
%%
%%
%% - Chat sample
%%      topic for 'notifications.rcp.netc.io', class 'ws'
%%      ws subscribes, app netcomp, obj_id con su uid
%%      notifications launches events for all uids
%%
%% - Actor sample
%%      topic for 'actors.netc.io', class is group:resource, type is event type
%%      can subscribe to all group:resource or one, one type or all
%%      it can use body to filter event types (created, etc.) on reception and namespace





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
%% - Empty fields (app, class, type, obj_id, namespace) means match any
%% - You SHOULD monitor the pid() and re-register if it fails
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
