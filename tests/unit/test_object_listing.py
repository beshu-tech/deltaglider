"""Unit tests for object_listing pagination."""

from unittest.mock import Mock

from deltaglider.core.object_listing import list_all_objects, list_objects_page


def test_list_objects_page_passes_continuation_token():
    """Test that list_objects_page passes continuation_token to storage."""
    storage = Mock()
    storage.list_objects.return_value = {
        "objects": [],
        "common_prefixes": [],
        "is_truncated": False,
        "next_continuation_token": None,
        "key_count": 0,
    }

    list_objects_page(
        storage,
        bucket="test-bucket",
        continuation_token="test-token",
    )

    # Verify continuation_token was passed
    storage.list_objects.assert_called_once()
    call_kwargs = storage.list_objects.call_args.kwargs
    assert call_kwargs["continuation_token"] == "test-token"


def test_list_all_objects_uses_continuation_token_for_pagination():
    """Test that list_all_objects uses continuation_token (not start_after) for pagination."""
    storage = Mock()

    # Mock 3 pages of results
    responses = [
        {
            "objects": [{"key": f"obj{i}"} for i in range(1000)],
            "common_prefixes": [],
            "is_truncated": True,
            "next_continuation_token": "token1",
            "key_count": 1000,
        },
        {
            "objects": [{"key": f"obj{i}"} for i in range(1000, 2000)],
            "common_prefixes": [],
            "is_truncated": True,
            "next_continuation_token": "token2",
            "key_count": 1000,
        },
        {
            "objects": [{"key": f"obj{i}"} for i in range(2000, 2500)],
            "common_prefixes": [],
            "is_truncated": False,
            "next_continuation_token": None,
            "key_count": 500,
        },
    ]

    storage.list_objects.side_effect = responses

    result = list_all_objects(
        storage,
        bucket="test-bucket",
        prefix="",
    )

    # Should have made 3 calls
    assert storage.list_objects.call_count == 3

    # Should have collected all objects
    assert len(result.objects) == 2500

    # Should not be truncated
    assert not result.is_truncated

    # Verify the calls used continuation_token correctly
    calls = storage.list_objects.call_args_list
    assert len(calls) == 3

    # First call should have no continuation_token
    assert calls[0].kwargs.get("continuation_token") is None

    # Second call should use token1
    assert calls[1].kwargs.get("continuation_token") == "token1"

    # Third call should use token2
    assert calls[2].kwargs.get("continuation_token") == "token2"


def test_list_all_objects_prevents_infinite_loop():
    """Test that list_all_objects has max_iterations protection."""
    storage = Mock()

    # Mock infinite pagination (always returns more)
    storage.list_objects.return_value = {
        "objects": [{"key": "obj"}],
        "common_prefixes": [],
        "is_truncated": True,
        "next_continuation_token": "token",
        "key_count": 1,
    }

    result = list_all_objects(
        storage,
        bucket="test-bucket",
        max_iterations=10,  # Low limit for testing
    )

    # Should stop at max_iterations
    assert storage.list_objects.call_count == 10
    assert result.is_truncated
