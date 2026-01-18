for script in Updates/All\ offers/*.py; do
  echo "Running $(basename "$script")"
  if ! python3 "$script"; then
    echo "❌ $(basename "$script") failed" >&2
  fi
done