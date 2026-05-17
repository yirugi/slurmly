"""0.5.0 — offset-based incremental log read, binary download, stateless attach.

Covers the enhancement-spec test matrix (groups 1-7), all against
FakeTransport / a mocked SFTP conn — no cluster.
"""

from __future__ import annotations

import pytest

from slurmly import (
    ArtifactDownload,
    DownloadResult,
    LogChunk,
    SlurmSSHClient,
    SubmittedJob,
)
from slurmly.exceptions import SSHTransportError
from slurmly.profiles import ClusterProfile
from slurmly.ssh.transport import AsyncSSHTransport, SSHConfig
from slurmly.testing import FakeTransport

BASE = "/scratch/u/slurmly"
JOB_DIR = f"{BASE}/jobs/slurmly-12345678"


def _client(transport: FakeTransport) -> SlurmSSHClient:
    profile = ClusterProfile(
        name="t",
        default_partition="shared",
        allowed_partitions=["shared"],
        sacct_lookback_days=14,
    )
    return SlurmSSHClient(
        transport=transport,
        cluster_profile=profile,
        remote_base_dir=BASE,
        account="a1",
    )


def _job() -> SubmittedJob:
    return SubmittedJob(
        internal_job_id="slurmly-12345678",
        slurm_job_id="42",
        remote_job_dir=JOB_DIR,
        remote_script_path=f"{JOB_DIR}/run.sh",
        stdout_path=f"{JOB_DIR}/stdout.log",
        stderr_path=f"{JOB_DIR}/stderr.log",
    )


# === group 1: read_bytes offset/length slicing ============================


async def test_read_bytes_offset_and_length_slicing():
    t = FakeTransport(files={"/f": b"0123456789"})
    assert await t.read_bytes("/f", offset=2, length=3) == b"234"
    assert await t.read_bytes("/f", offset=0, length=None) == b"0123456789"
    assert await t.read_bytes("/f", offset=5) == b"56789"


async def test_read_bytes_offset_eq_size_is_empty_and_past_size_ok():
    t = FakeTransport(files={"/f": b"0123456789"})
    assert await t.read_bytes("/f", offset=10) == b""  # offset == size
    assert await t.read_bytes("/f", offset=20) == b""  # offset > size


async def test_read_bytes_missing_raises_ssh_transport_error():
    t = FakeTransport()
    with pytest.raises(SSHTransportError):
        await t.read_bytes("/nope")


async def test_stat_size_reports_len_or_none():
    t = FakeTransport(files={"/f": b"abcd"})
    assert await t.stat_size("/f") == 4
    assert await t.stat_size("/missing") is None


# === group 2: read_log absent / growth / truncation =======================


async def test_read_log_absent_file():
    client = _client(FakeTransport())
    chunk = await client.read_log("/var/log/run.log", offset=7)
    assert isinstance(chunk, LogChunk)
    assert chunk.exists is False
    assert chunk.content == ""
    assert chunk.next_offset == 7  # cursor preserved
    assert chunk.size is None
    assert chunk.note


async def test_read_log_growth_has_no_loss_or_dupe():
    t = FakeTransport(files={"/log": b"abc"})
    client = _client(t)

    first = await client.read_log("/log", offset=0)
    assert first.content == "abc"
    assert first.next_offset == 3
    assert first.size == 3
    assert first.truncated is False

    t.append("/log", b"def")
    second = await client.read_log("/log", offset=first.next_offset)
    assert second.content == "def"
    assert second.next_offset == 6
    assert first.content + second.content == "abcdef"


async def test_read_log_truncation_resets_cursor():
    t = FakeTransport(files={"/log": b"0123456789"})
    client = _client(t)
    # Persisted cursor (20) is now past the (rotated, shorter) file.
    chunk = await client.read_log("/log", offset=20)
    assert chunk.truncated is True
    assert chunk.content == "0123456789"  # re-read from 0
    assert chunk.next_offset == 10


async def test_read_log_decodes_partial_multibyte_with_replace():
    # A lone UTF-8 lead byte (first byte of 'é'): must not crash the decode.
    t = FakeTransport(files={"/log": b"\xc3"})
    chunk = await _client(t).read_log("/log", offset=0)
    assert chunk.exists is True
    assert chunk.content == "�"  # U+FFFD replacement


async def test_read_log_selects_stream_from_submitted_job():
    job = _job()
    t = FakeTransport(files={job.stderr_path: b"boom"})
    chunk = await _client(t).read_log(job, stream="stderr")
    assert chunk.content == "boom"
    assert chunk.path == job.stderr_path


# === group 3: binary download (download_file / download_artifact) =========

NUL_BLOB = b"\x00\x01PK\x03\x04binary\x00\xff\xfe payload"


async def test_download_file_writes_exact_bytes(tmp_path):
    remote = f"{JOB_DIR}/model.sl4"
    t = FakeTransport(files={remote: NUL_BLOB})
    client = _client(t)
    local = tmp_path / "out" / "model.sl4"

    res = await client.download_file(remote, str(local))
    assert isinstance(res, DownloadResult)
    assert res.bytes_written == len(NUL_BLOB)
    assert res.remote_path == remote
    assert local.read_bytes() == NUL_BLOB  # NUL-safe, parents created


async def test_download_artifact_binary_returns_exact_bytes():
    t = FakeTransport(files={f"{JOB_DIR}/model.har": NUL_BLOB})
    res = await _client(t).download_artifact(_job(), "model.har", binary=True)
    assert isinstance(res, ArtifactDownload)
    assert res.content_bytes == NUL_BLOB
    assert res.content == ""


async def test_download_artifact_text_default_unchanged_regression():
    t = FakeTransport(files={f"{JOB_DIR}/metrics.json": '{"loss": 0.1}'})
    res = await _client(t).download_artifact(_job(), "metrics.json")
    assert res.content == '{"loss": 0.1}'
    assert res.content_bytes is None  # text path leaves bytes unset


# === group 4: follow_log incremental yields + final flush + cancel ========


async def test_follow_log_yields_ordered_gapless_chunks_until_flip():
    t = FakeTransport(files={"/log": b"line1\n"})
    client = _client(t)
    calls = {"n": 0}

    async def until() -> bool:
        calls["n"] += 1
        if calls["n"] == 1:
            t.append("/log", b"line2\n")
            return False
        t.append("/log", b"line3\n")  # appears just before stop
        return True

    chunks = [
        c
        async for c in client.follow_log(
            "/log", poll_interval=0, idle_grace=0, until=until
        )
    ]
    joined = "".join(c.content for c in chunks)
    assert joined == "line1\nline2\nline3\n"  # ordered, no gaps, final flush
    # cursor advances monotonically across yields
    offsets = [c.next_offset for c in chunks]
    assert offsets == sorted(offsets)


async def test_follow_log_keeps_polling_while_file_absent():
    t = FakeTransport()
    client = _client(t)
    calls = {"n": 0}

    async def until() -> bool:
        calls["n"] += 1
        if calls["n"] == 2:
            t.files["/log"] = b"ready\n"  # appears only after a few polls
        return calls["n"] >= 3

    chunks = [
        c
        async for c in client.follow_log(
            "/log", poll_interval=0, idle_grace=0, until=until
        )
    ]
    assert "".join(c.content for c in chunks) == "ready\n"


async def test_follow_log_clean_cancel_via_aclose():
    t = FakeTransport(files={"/log": b"data\n"})
    gen = _client(t).follow_log("/log", poll_interval=0, idle_grace=0)
    first = await gen.__anext__()
    assert first.content == "data\n"
    await gen.aclose()  # GeneratorExit propagates cleanly, no raise


# === group 5: attach + SubmittedJob round-trip ============================


def test_attach_builds_minimal_job_with_empty_internal_id():
    job = _client(FakeTransport()).attach(
        "98765",
        remote_job_dir=JOB_DIR,
        stdout_path=f"{JOB_DIR}/stdout.log",
        stderr_path=f"{JOB_DIR}/stderr.log",
        cluster="anvil",
    )
    assert job.internal_job_id == ""
    assert job.slurm_job_id == "98765"
    assert job.cluster == "anvil"


def test_submitted_job_model_dump_validate_round_trip():
    job = _client(FakeTransport()).attach(
        "98765",
        remote_job_dir=JOB_DIR,
        stdout_path=f"{JOB_DIR}/stdout.log",
        stderr_path=f"{JOB_DIR}/stderr.log",
    )
    restored = SubmittedJob.model_validate(job.model_dump())
    assert restored == job


async def test_read_log_via_reconstructed_handle():
    out = f"{JOB_DIR}/stdout.log"
    t = FakeTransport(files={out: b"progress 50%\n"})
    client = _client(t)
    job = client.attach(
        "98765",
        remote_job_dir=JOB_DIR,
        stdout_path=out,
        stderr_path=f"{JOB_DIR}/stderr.log",
    )
    chunk = await client.read_log(job)
    assert chunk.content == "progress 50%\n"


# === group 6: heredoc transport — upload_file raises, read_bytes works ====


class _FakeSFTPFile:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def __aenter__(self) -> _FakeSFTPFile:
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def read(self, size: int = -1, offset: int | None = None) -> bytes:
        start = offset or 0
        if size is None or size < 0:
            return self._data[start:]
        return self._data[start : start + size]


class _FakeSFTPClient:
    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files

    async def __aenter__(self) -> _FakeSFTPClient:
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    def open(self, path: str, mode: str = "r") -> _FakeSFTPFile:
        if path not in self._files:
            raise FileNotFoundError(path)
        return _FakeSFTPFile(self._files[path])


class _FakeSFTPConn:
    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files

    def start_sftp_client(self) -> _FakeSFTPClient:
        return _FakeSFTPClient(self._files)


def _heredoc_cfg() -> SSHConfig:
    return SSHConfig(
        host="h", username="u", key_path="/k", upload_method="heredoc"
    )


async def test_heredoc_upload_file_raises():
    t = AsyncSSHTransport(_heredoc_cfg())
    with pytest.raises(SSHTransportError, match="requires SFTP"):
        await t.upload_file("/tmp/whatever", "/scratch/x.bin")


async def test_heredoc_read_bytes_still_uses_sftp(monkeypatch):
    t = AsyncSSHTransport(_heredoc_cfg())
    conn = _FakeSFTPConn({"/scratch/run.log": b"0123456789"})

    async def fake_ensure_conn():
        t._conn = conn
        return conn

    monkeypatch.setattr(t, "_ensure_conn", fake_ensure_conn)
    assert await t.read_bytes("/scratch/run.log", offset=3, length=4) == b"3456"
    assert await t.read_bytes("/scratch/run.log") == b"0123456789"


# === group 7: back-compat (read_text unchanged) ===========================


async def test_read_text_str_seeded_unchanged():
    t = FakeTransport(files={"/f": "hello world"})
    assert await t.read_text("/f") == "hello world"
    assert await t.read_text("/f", max_bytes=5) == "hello"


async def test_read_text_missing_still_raises_filenotfound():
    t = FakeTransport()
    with pytest.raises(FileNotFoundError):
        await t.read_text("/nope")


async def test_read_text_coerces_bytes_value_without_breaking():
    t = FakeTransport(files={"/f": b"caf\xc3\xa9"})  # 'café' utf-8
    assert await t.read_text("/f") == "café"


async def test_fake_upload_file_and_makedirs_recorded(tmp_path):
    src = tmp_path / "blob.bin"
    src.write_bytes(NUL_BLOB)
    t = FakeTransport()
    await t.makedirs("/scratch/u/slurmly/jobs/x", mode=0o700)
    await t.upload_file(str(src), "/remote/blob.bin")
    assert "/scratch/u/slurmly/jobs/x" in t.made_dirs
    assert t.files["/remote/blob.bin"] == NUL_BLOB
    assert t.uploads[-1].path == "/remote/blob.bin"
