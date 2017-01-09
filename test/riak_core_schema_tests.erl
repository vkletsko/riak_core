%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2017 Basho Technologies, Inc.
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

-module(riak_core_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

%% basic schema test will check to make sure that all defaults from
%% the schema make it into the generated app.config
basic_schema_test() ->
    %% The defaults are defined in ../priv/riak_core.schema. it is the
    %% file under test.
    PrivDir = cuttlefish_unit:lib_priv_dir(riak_core),
    ?assertNotEqual(false, PrivDir),
    Config = cuttlefish_unit:generate_templated_config(
               filename:join(PrivDir, "riak_core.schema"), [], context()),

    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.n_val", 3),
    cuttlefish_unit:assert_config(Config, "riak_core.ring_creation_size", 64),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_concurrency", 2),
    cuttlefish_unit:assert_config(Config, "riak_core.ring_state_dir", "./data/ring"),
    cuttlefish_unit:assert_not_configured(Config, "riak_core.ssl.certfile"),
    cuttlefish_unit:assert_not_configured(Config, "riak_core.ssl.keyfile"),
    cuttlefish_unit:assert_not_configured(Config, "riak_core.ssl.cacertfile"),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_ip", "0.0.0.0"),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_port", 8099 ),
    cuttlefish_unit:assert_not_configured(Config, "riak_core.handoff_ssl_options"),
    cuttlefish_unit:assert_config(Config, "riak_core.dtrace_support", false),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_bin_dir", "./bin"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_data_dir", "./data"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_etc_dir", "./etc"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_lib_dir", "./lib"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_log_dir", "./log"),
    cuttlefish_unit:assert_config(Config, "riak_core.enable_consensus", false),
    cuttlefish_unit:assert_config(Config, "riak_core.use_background_manager", false),
    cuttlefish_unit:assert_config(Config, "riak_core.vnode_management_timer", 10000),
    ok.

%% Tests that configurations which should be prohibited by validators defined
%% in the schema are, in fact, reported as invalid.
invalid_states_test() ->
    Conf = [
        {["handoff", "ip"], "0.0.0.0.0"}
    ],

    PrivDir = cuttlefish_unit:lib_priv_dir(riak_core),
    ?assertNotEqual(false, PrivDir),
    Config = cuttlefish_unit:generate_templated_config(
        filename:join(PrivDir, "riak_core.schema"), Conf, context()),

    %% Confirm that we made it to validation and test that each expected failure
    %% message is present.
    cuttlefish_unit:assert_error_in_phase(Config, validation),
    cuttlefish_unit:assert_error_message(Config, "handoff.ip invalid, must be a valid IP address"),
    ok.


default_bucket_properties_test() ->
    Conf = [
        {["buckets", "default", "n_val"], 5}
    ],

    PrivDir = cuttlefish_unit:lib_priv_dir(riak_core),
    ?assertNotEqual(false, PrivDir),
    Config = cuttlefish_unit:generate_templated_config(
        filename:join(PrivDir, "riak_core.schema"), Conf, context()),

    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.n_val", 5),
    ok.

override_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by cuttlefish.
    %% this proplists is what would be output by the conf_parse module
    Conf = [
        {["buckets", "default", "n_val"], 4},
        {["ring_size"], 8},
        {["transfer_limit"], 4},
        {["ring", "state_dir"], "/absolute/ring"},
        {["ssl", "certfile"], "/absolute/etc/cert.pem"},
        {["ssl", "keyfile"], "/absolute/etc/key.pem"},
        {["ssl", "cacertfile"], "/absolute/etc/cacertfile.pem"},
        {["handoff", "ip"], "1.2.3.4"},
        {["handoff", "port"], 8888},
        {["handoff", "ssl", "certfile"], "/tmp/erlserver.pem"},
        {["handoff", "ssl", "keyfile"], "/tmp/erlkey/pem"},
        {["dtrace"], on},
        %% Platform-specific installation paths (substituted by rebar)
        {["platform_bin_dir"], "/absolute/bin"},
        {["platform_data_dir"],"/absolute/data" },
        {["platform_etc_dir"], "/absolute/etc"},
        {["platform_lib_dir"], "/absolute/lib"},
        {["platform_log_dir"], "/absolute/log"},
        {["strong_consistency"], on},
        {["background_manager"], on},
        {["vnode_management_timer"], "20s"}
    ],

    PrivDir = cuttlefish_unit:lib_priv_dir(riak_core),
    ?assertNotEqual(false, PrivDir),
    Config = cuttlefish_unit:generate_templated_config(
        filename:join(PrivDir, "riak_core.schema"), Conf, context()),

    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.n_val", 4),
    cuttlefish_unit:assert_config(Config, "riak_core.ring_creation_size", 8),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_concurrency", 4),
    cuttlefish_unit:assert_config(Config, "riak_core.ring_state_dir", "/absolute/ring"),
    cuttlefish_unit:assert_config(Config, "riak_core.ssl.certfile", "/absolute/etc/cert.pem"),
    cuttlefish_unit:assert_config(Config, "riak_core.ssl.keyfile", "/absolute/etc/key.pem"),
    cuttlefish_unit:assert_config(Config, "riak_core.ssl.cacertfile", "/absolute/etc/cacertfile.pem"),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_ip", "1.2.3.4"),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_port", 8888),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_ssl_options.certfile", "/tmp/erlserver.pem"),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_ssl_options.keyfile", "/tmp/erlkey/pem"),
    cuttlefish_unit:assert_config(Config, "riak_core.dtrace_support", true),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_bin_dir", "/absolute/bin"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_data_dir", "/absolute/data"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_etc_dir", "/absolute/etc"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_lib_dir", "/absolute/lib"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_log_dir", "/absolute/log"),
    cuttlefish_unit:assert_config(Config, "riak_core.enable_consensus", true),
    cuttlefish_unit:assert_config(Config, "riak_core.use_background_manager", true),
    cuttlefish_unit:assert_config(Config, "riak_core.vnode_management_timer", 20000),
    ok.

%% this context() represents the substitution variables that rebar
%% will use during the build process.  riak_core's schema file is
%% written with some {{mustache_vars}} for substitution during
%% packaging cuttlefish doesn't have a great time parsing those, so we
%% perform the substitutions first, because that's how it would work
%% in real life.
context() ->
    [
        {handoff_ip, "0.0.0.0"},
        {handoff_port, "8099"},
        {platform_bin_dir , "./bin"},
        {platform_data_dir, "./data"},
        {platform_etc_dir , "./etc"},
        {platform_lib_dir , "./lib"},
        {platform_log_dir , "./log"}
    ].
