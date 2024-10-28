# %%
kwargs = {"test": 0}

# %%
assert "test" in kwargs

# %%
try:
    assert "jose" in kwargs
except Exception as exc:
    print(exc)
    assert isinstance(exc, AssertionError)
    print("Assertou")

# %%
print(kwargs.get("test"))
