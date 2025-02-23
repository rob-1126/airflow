# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import botocore.exceptions
import pytest
from moto import mock_ssm
from pytest import param

from airflow.providers.amazon.aws.hooks.ssm import SsmHook

DEFAULT_CONN_ID: str = "aws_default"
REGION: str = "us-east-1"

EXISTING_PARAM_NAME = "parameter"
BAD_PARAM_NAME = "parameter_does_not_exist"
PARAM_VALUE = "value"
DEFAULT_VALUE = "default"


class TestSsmHooks:
    @pytest.fixture(autouse=True)
    def setup_tests(self):
        with mock_ssm():
            self.hook = SsmHook(region_name=REGION)
            self.hook.conn.put_parameter(
                Type="String", Name=EXISTING_PARAM_NAME, Value=PARAM_VALUE, Overwrite=True
            )
            yield

    def test_hook(self) -> None:
        assert self.hook.conn is not None
        assert self.hook.aws_conn_id == DEFAULT_CONN_ID
        assert self.hook.region_name == REGION

    @pytest.mark.parametrize(
        "param_name, default_value, expected_result",
        [
            param(EXISTING_PARAM_NAME, None, PARAM_VALUE, id="param_exists_no_default_provided"),
            param(EXISTING_PARAM_NAME, DEFAULT_VALUE, PARAM_VALUE, id="param_exists_with_default"),
            param(BAD_PARAM_NAME, DEFAULT_VALUE, DEFAULT_VALUE, id="param_does_not_exist_uses_default"),
        ],
    )
    def test_get_parameter_value_happy_cases(self, param_name, default_value, expected_result) -> None:
        if default_value:
            assert self.hook.get_parameter_value(param_name, default=default_value) == expected_result
        else:
            assert self.hook.get_parameter_value(param_name) == expected_result

    def test_get_parameter_value_param_does_not_exist_no_default_provided(self) -> None:
        with pytest.raises(botocore.exceptions.ClientError) as raised_exception:
            self.hook.get_parameter_value(BAD_PARAM_NAME)

        error = raised_exception.value.response["Error"]
        assert error["Code"] == "ParameterNotFound"
        assert BAD_PARAM_NAME in error["Message"]
