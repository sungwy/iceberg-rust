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

import os
import pytest
from pyiceberg_core import fileio

def test_inputfile():
    path = "file:/haha.txt"
    input_file = fileio.InputFile(path)
    assert input_file.location() == path
    
@pytest.mark.asyncio
async def test_inputfile_async_funcs():
    os.remove("/tmp/haha.txt")
    
    path = "file:/tmp/haha.txt"
    input_file = fileio.InputFile(path)
    assert input_file.location() == path
    assert await input_file.exists() == False
    
    body = "This is the first line.\n"
    with open("/tmp/haha.txt", "w") as file:
        file.write(body)
        
    input_file = fileio.InputFile(path)
    
    # assert await input_file.exists() == True
    
    assert await input_file.read() == body.encode()
