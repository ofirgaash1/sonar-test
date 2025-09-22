"""Legacy shims for transcript helpers.

This module now delegates to the functions defined under ``explore.app.transcripts``.
Prefer importing from those modules directly; this file remains for backwards
compatibility with older code paths.
"""
from __future__ import annotations

from ..transcripts import alignment as alignment_utils
from ..transcripts import db_ops
from ..transcripts import schema
from ..transcripts import text_ops
from ..transcripts import timing
from ..transcripts import utils

ensure_schema = schema.ensure_schema
_carry_over_timings_from_db = timing.carry_over_timings_from_db
_carry_over_timings = text_ops.carry_over_token_timings
_tokenize_text_to_words = text_ops.tokenize_text_to_words
_validate_and_sanitize_words = text_ops.validate_and_sanitize_words
_ensure_words_match_text = text_ops.ensure_words_match_text
_canonicalize_text = text_ops.canonicalize_text
_compose_text_from_words = text_ops.compose_text_from_words
_compose_full_text_from_words = text_ops.compose_full_text_from_words
_diff = text_ops.diff_text
_detect_changed_segments = text_ops.detect_changed_segments
_build_segment_filter = db_ops.build_segment_filter
_normalize_db_words_rows = db_ops.normalize_db_words_rows
_slice_words_json = db_ops.slice_words_json
_normalize_words_json_all = db_ops.normalize_words_json_all
_latest_row = db_ops.latest_row
_row_for_version = db_ops.row_for_version
_populate_transcript_words = db_ops.populate_transcript_words
_normalize_end_times = db_ops.normalize_end_times
_fetch_prev_rows_for_window = db_ops.fetch_words_rows

_clamp_neighbors = utils.clamp_neighbors
_log_info = utils.log_info
_ensure_safe_doc = utils.ensure_safe_doc
_sha256_hex = utils.sha256_hex
_safe_name = utils.safe_name

_maybe_deref_audio_pointer = alignment_utils.maybe_deref_audio_pointer
_ffmpeg_extract_wav_clip = alignment_utils.ffmpeg_extract_wav_clip
_align_call = alignment_utils.align_call
_save_alignment_artifacts = alignment_utils.save_alignment_artifacts
_build_new_window = alignment_utils.build_new_window
_map_aligned_to_updates = alignment_utils.map_aligned_to_updates
_compute_clip_from_prev_rows = alignment_utils.compute_clip_from_prev_rows
_explode_resp_words_if_needed = alignment_utils.explode_resp_words_if_needed
_segment_window = alignment_utils.segment_window
_prealign_updates = alignment_utils.prealign_updates

__all__ = [
    "ensure_schema",
    "_carry_over_timings_from_db",
    "_carry_over_timings",
    "_tokenize_text_to_words",
    "_validate_and_sanitize_words",
    "_ensure_words_match_text",
    "_canonicalize_text",
    "_compose_text_from_words",
    "_compose_full_text_from_words",
    "_diff",
    "_detect_changed_segments",
    "_build_segment_filter",
    "_normalize_db_words_rows",
    "_slice_words_json",
    "_normalize_words_json_all",
    "_latest_row",
    "_row_for_version",
    "_populate_transcript_words",
    "_normalize_end_times",
    "_fetch_prev_rows_for_window",
    "_clamp_neighbors",
    "_log_info",
    "_ensure_safe_doc",
    "_sha256_hex",
    "_safe_name",
    "_maybe_deref_audio_pointer",
    "_ffmpeg_extract_wav_clip",
    "_align_call",
    "_save_alignment_artifacts",
    "_build_new_window",
    "_map_aligned_to_updates",
    "_compute_clip_from_prev_rows",
    "_explode_resp_words_if_needed",
    "_segment_window",
    "_prealign_updates",
]
