
Filename: coastal-radar/multithread-vectors-fetcher.py

This script downloads and manages hourly coastal radar vector data products from Ocean Networks Canada (ONC) using the ONC Python SDK. It supports multi-threaded downloads, periodic manifest saving, and mid-run recovery after interruptions.

Features

    Multi-threaded downloading (default: 10 worker threads)

    Download manifest (manifest.csv) tracks each file and its status (success / failed)

    Automatic retry on rerun: only missing or failed files are re-queued

    Periodic manifest saving (default: every 90 seconds)

    Provenance file (provenance.yaml) records API calls and dataset context

    Mid-run interrupt support: rerun safely after stopping

Download Layout

coastal-radar/
    data/
        vectors(2024)/          # downloaded .tuv files
    metadata/
        vectors(2024)/
            manifest.csv        # download status tracking
            provenance.yaml     # provenance and API metadata

Manifest Behavior

    manifest.csv is the record of all attempted downloads.

On rerun:

    success → skipped

    failed or missing → retried

    Manifest is updated continuously during execution.

Reruns and Interruptions

    If the script stops or fails, rerun with the same time period.

    Already successful files are skipped.

    Failed files are retried automatically.

Changing Time Periods

    Using the same period repeatedly is safe.

    Changing the period without clearing the manifest will merge old and new requests. Old files remain in the manifest, new ones are added.

Best practice for testing:

    Run on a short subset of the intended period (e.g. one week).

    If successful, re-run for the full period.

Resetting Progress
    
    To force a complete re-download:

        Delete metadata/vectors(YYYY)/manifest.csv.

        Optionally delete any files in data/vectors(YYYY)/.

        Re-run with desired time period.

Concurrency and Retry Logic

    Downloads run in parallel worker threads.

    A queue distributes files to workers.

    Each worker updates the manifest with success or failed status.

    Failed files remain in the manifest and are retried on rerun.

    A background thread saves the manifest periodically to preserve progress.

Best Practices

    Keep the time period fixed for production runs.

    For testing, run a small subset first, then clear manifest and files before a full run.

    Rerun with the same date range if recovering from an interruption.