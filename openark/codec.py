import json
from typing import Any, Optional

import msgpack


def dumps(data: dict[str, Any], codec: str) -> bytes | bytearray:
    match codec:
        case 'MessagePack':
            return bytearray([_OPCODE_MESSAGEPACK]) + msgpack.dumps(data)
        case 'Json':
            return json.dumps(data).encode('utf-8')
        case _:
            raise Exception(f'Unknown encoding codec: {codec}')


def loads(data: bytes) -> Optional[dict[str, Any]]:
    if not data:
        raise Exception('Empty data')

    opcode = data[0]
    if opcode <= _OPCODE_ASCIIEND:
        try:
            return json.loads(data)
        except json.decoder.JSONDecodeError:
            return None
    elif opcode == _OPCODE_MESSAGEPACK:
        try:
            return msgpack.loads(data[1:])
        except msgpack.exceptions.UnpackException:
            return None
    else:
        raise Exception(f'cannot infer serde opcode')

##############
#   OpCode   #
##############


# NOTE: The opcode for text serde should be in ASCII
_OPCODE_ASCIIEND = 0x7F

# NOTE: The opcodes for binary serde should be in extended ASCII
_OPCODE_MESSAGEPACK = 0x80
_OPCODE_CBOR = 0x81
