from __future__ import annotations

import logging
import re
import zlib
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from xml.sax.saxutils import escape

from backend.app.domains.downloads.postprocessing.payloads import _publish_bytes
from backend.app.domains.downloads.postprocessing.tags import _tag_text

logger = logging.getLogger(__name__)


_XMP_APP1_HEADER = b"http://ns.adobe.com/xap/1.0/\x00"


_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


_PNG_XMP_KEYWORD = b"XML:com.adobe.xmp\x00"


_WEBP_XMP_FLAG = 0x04


# Characters XML 1.0 cannot carry, the only ones dropped from XMP tags.
_XML_ILLEGAL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f￾￿\ud800-\udfff]")


def _xmp_text(value: Any) -> str:
    return escape(_XML_ILLEGAL_RE.sub("", _tag_text(value)), {'"': "&quot;", "'": "&apos;"})


def _xmp_alt(name: str, value: Any) -> str:
    text = _xmp_text(value)
    return (
        f'<dc:{name}><rdf:Alt><rdf:li xml:lang="x-default">{text}</rdf:li>'
        f"</rdf:Alt></dc:{name}>"
        if text
        else ""
    )


def _xmp_sequence(name: str, *values: Any) -> str:
    items = "".join(f"<rdf:li>{text}</rdf:li>" for value in values if (text := _xmp_text(value)))
    return f"<dc:{name}><rdf:Seq>{items}</rdf:Seq></dc:{name}>" if items else ""


def _xmp_bag(name: str, *values: Any) -> str:
    items = "".join(f"<rdf:li>{text}</rdf:li>" for value in values if (text := _xmp_text(value)))
    return f"<dc:{name}><rdf:Bag>{items}</rdf:Bag></dc:{name}>" if items else ""


def _image_xmp_packet(tags: dict[str, str]) -> bytes:
    """Build standard Dublin Core/XMP Rights metadata, with no app-specific fields."""
    description = tags.get("description") or tags.get("comment")
    source = tags.get("source") or tags.get("comment")
    standard = "".join(
        [
            _xmp_alt("title", tags.get("title")),
            _xmp_sequence("creator", tags.get("artist") or tags.get("album_artist")),
            _xmp_alt("description", description),
            _xmp_alt("rights", tags.get("copyright")),
            _xmp_sequence("date", tags.get("date")),
            _xmp_bag("publisher", tags.get("publisher")),
            _xmp_bag("language", tags.get("language")),
            _xmp_bag("subject", tags.get("keywords"), tags.get("genre")),
            f"<dc:source>{_xmp_text(source)}</dc:source>" if source else "",
            (
                f"<dc:identifier>{_xmp_text(tags.get('identifier'))}</dc:identifier>"
                if tags.get("identifier")
                else ""
            ),
            (
                f"<xmpRights:WebStatement>{_xmp_text(source)}</xmpRights:WebStatement>"
                if source and urlparse(source).scheme.lower() in {"http", "https"}
                else ""
            ),
        ]
    )
    packet = (
        '<?xpacket begin="\ufeff" id="W5M0MpCehiHzreSzNTczkc9d"?>'
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        '<rdf:Description rdf:about="" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:xmpRights="http://ns.adobe.com/xap/1.0/rights/">'
        f"{standard}"
        "</rdf:Description></rdf:RDF></x:xmpmeta>"
        '<?xpacket end="w"?>'
    )
    return packet.encode("utf-8")


def _jpeg_with_xmp(data: bytes, xmp: bytes) -> bytes | None:
    if not data.startswith(b"\xff\xd8"):
        return None
    app1_payload = _XMP_APP1_HEADER + xmp
    if len(app1_payload) + 2 > 0xFFFF:
        return None
    replacement = b"\xff\xe1" + (len(app1_payload) + 2).to_bytes(2, "big") + app1_payload
    output = bytearray(data[:2])
    position = 2
    inserted = False
    while position < len(data):
        marker_start = position
        if data[position] != 0xFF:
            return None
        while position < len(data) and data[position] == 0xFF:
            position += 1
        if position >= len(data):
            return None
        marker = data[position]
        position += 1
        if marker in {0xD9, 0xDA}:
            if not inserted:
                output.extend(replacement)
                inserted = True
            output.extend(data[marker_start:])
            break
        if marker in {0x01, *range(0xD0, 0xD8)}:
            output.extend(data[marker_start:position])
            continue
        if position + 2 > len(data):
            return None
        segment_length = int.from_bytes(data[position : position + 2], "big")
        segment_end = position + segment_length
        if segment_length < 2 or segment_end > len(data):
            return None
        segment = data[marker_start:segment_end]
        is_xmp = marker == 0xE1 and data[position + 2 : segment_end].startswith(_XMP_APP1_HEADER)
        if not inserted and marker != 0xE0:
            output.extend(replacement)
            inserted = True
        if not is_xmp:
            output.extend(segment)
        position = segment_end
    return bytes(output) if inserted else None


def _png_chunk(kind: bytes, payload: bytes) -> bytes:
    checksum = zlib.crc32(kind + payload) & 0xFFFFFFFF
    return len(payload).to_bytes(4, "big") + kind + payload + checksum.to_bytes(4, "big")


def _png_with_xmp(data: bytes, xmp: bytes) -> bytes | None:
    if not data.startswith(_PNG_SIGNATURE):
        return None
    replacement = _png_chunk(b"iTXt", _PNG_XMP_KEYWORD + b"\x00\x00\x00\x00" + xmp)
    output = bytearray(_PNG_SIGNATURE)
    position = len(_PNG_SIGNATURE)
    inserted = False
    while position + 12 <= len(data):
        chunk_length = int.from_bytes(data[position : position + 4], "big")
        chunk_end = position + 12 + chunk_length
        if chunk_end > len(data):
            return None
        kind = data[position + 4 : position + 8]
        payload = data[position + 8 : position + 8 + chunk_length]
        is_xmp = kind == b"iTXt" and payload.startswith(_PNG_XMP_KEYWORD)
        if kind == b"IEND" and not inserted:
            output.extend(replacement)
            inserted = True
        if not is_xmp:
            output.extend(data[position:chunk_end])
        position = chunk_end
        if kind == b"IEND":
            break
    return bytes(output) if inserted and position == len(data) else None


def _webp_chunk(kind: bytes, payload: bytes) -> bytes:
    padding = b"\x00" if len(payload) % 2 else b""
    return kind + len(payload).to_bytes(4, "little") + payload + padding


def _webp_canvas(chunks: list[tuple[bytes, bytes]]) -> tuple[int, int, bool] | None:
    for kind, payload in chunks:
        if kind == b"VP8 " and len(payload) >= 10 and payload[3:6] == b"\x9d\x01\x2a":
            width = int.from_bytes(payload[6:8], "little") & 0x3FFF
            height = int.from_bytes(payload[8:10], "little") & 0x3FFF
            return (width, height, False) if width and height else None
        if kind == b"VP8L" and len(payload) >= 5 and payload[0] == 0x2F:
            bits = int.from_bytes(payload[1:5], "little")
            width = (bits & 0x3FFF) + 1
            height = ((bits >> 14) & 0x3FFF) + 1
            return width, height, bool(bits & (1 << 28))
    return None


def _webp_with_xmp(data: bytes, xmp: bytes) -> bytes | None:
    if len(data) < 12 or data[:4] != b"RIFF" or data[8:12] != b"WEBP":
        return None
    declared_end = int.from_bytes(data[4:8], "little") + 8
    if declared_end != len(data):
        return None

    chunks: list[tuple[bytes, bytes]] = []
    position = 12
    while position + 8 <= len(data):
        kind = data[position : position + 4]
        size = int.from_bytes(data[position + 4 : position + 8], "little")
        payload_end = position + 8 + size
        chunk_end = payload_end + (size % 2)
        if chunk_end > len(data):
            return None
        if kind != b"XMP ":
            chunks.append((kind, data[position + 8 : payload_end]))
        position = chunk_end
    if position != len(data):
        return None

    extended_index = next((index for index, (kind, _) in enumerate(chunks) if kind == b"VP8X"), None)
    if extended_index is None:
        canvas = _webp_canvas(chunks)
        if canvas is None:
            return None
        width, height, has_alpha = canvas
        flags = _WEBP_XMP_FLAG
        flags |= 0x20 if any(kind == b"ICCP" for kind, _ in chunks) else 0
        flags |= 0x10 if has_alpha or any(kind == b"ALPH" for kind, _ in chunks) else 0
        flags |= 0x08 if any(kind == b"EXIF" for kind, _ in chunks) else 0
        flags |= 0x02 if any(kind in {b"ANIM", b"ANMF"} for kind, _ in chunks) else 0
        extended = bytes([flags]) + b"\x00\x00\x00"
        extended += (width - 1).to_bytes(3, "little") + (height - 1).to_bytes(3, "little")
        chunks.insert(0, (b"VP8X", extended))
    else:
        payload = chunks[extended_index][1]
        if len(payload) != 10:
            return None
        chunks[extended_index] = (b"VP8X", bytes([payload[0] | _WEBP_XMP_FLAG]) + payload[1:])

    chunks.append((b"XMP ", xmp))
    body = b"WEBP" + b"".join(_webp_chunk(kind, payload) for kind, payload in chunks)
    return b"RIFF" + len(body).to_bytes(4, "little") + body


def _lossless_xmp_writer(data: bytes) -> Any:
    """Select by file signature so extractor names and source sites are irrelevant."""
    if data.startswith(b"\xff\xd8"):
        return _jpeg_with_xmp
    if data.startswith(_PNG_SIGNATURE):
        return _png_with_xmp
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return _webp_with_xmp
    return None


def _embed_image_metadata(path: Path, tags: dict[str, str]) -> bool:
    try:
        data = path.read_bytes()
        writer = _lossless_xmp_writer(data)
        embedded = writer(data, _image_xmp_packet(tags)) if writer is not None else None
        if embedded is None:
            return False
        _publish_bytes(path, embedded)
        return True
    except OSError as exc:
        logger.warning("Metadata embed skipped for %s: %s", path, exc)
        return False
