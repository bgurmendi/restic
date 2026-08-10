..
  Normally, there are no heading levels assigned to certain characters as the structure is
  determined from the succession of headings. However, this convention is used in Python’s
  Style Guide for documenting which you may follow:

  # with overline, for parts
  * for chapters
  = for sections
  - for subsections
  ^ for subsubsections
  " for paragraphs

####################################
Object Lock support (EXPERIMENTAL)
####################################

.. warning:: This feature is EXPERIMENTAL (Alpha state) and requires the ``object-lock``
    feature flag to be enabled, see `Enabling the feature flag`_ below. Behavior may change
    in arbitrary ways between restic versions, including in backwards-incompatible ways, or
    be removed entirely. Do not build critical infrastructure around it yet.

This feature lets a restic repository stored on an S3-compatible backend use `S3 Object
Lock`_ so that the data a retention policy currently wants to keep cannot be deleted early,
even by someone holding delete-capable credentials for the bucket (for example an attacker
who has compromised the backup client, or an insider with delete access). It currently
supports the ``s3`` backend only.

.. _S3 Object Lock: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html

The problem: deduplication vs. object creation time
****************************************************

A naive way to add Object Lock support would be to lock every object for a fixed period at
the moment it is written, and rely on that alone. This does not work correctly with a
deduplicating backup tool like restic.

Consider a pack file uploaded on 2026-01-01, whose Object Lock retention would then expire
on 2026-05-01. Suppose that pack contains the data for ``foto_2015.jpg`` and
``video_2016.mp4``. Those two files can easily still be part of the most recent snapshot
years later, since restic reuses existing blobs and packs instead of re-uploading unchanged
data. If the pack's lock is tied only to when it was *created*, its retention can expire and
the pack can be deleted long before the snapshots that still need it are gone -- exactly the
scenario Object Lock was meant to prevent.

The retention that matters has to be tied to:

.. code-block:: text

    snapshots you want to keep
        ↓
    objects needed to restore them
        ↓
    retention on those objects

and not to when an object happened to be created. Because computing "objects needed to
restore the snapshots a policy currently keeps" is exactly what ``forget``'s snapshot
selection and ``prune``'s used-blob/pack computation already do, this feature does not
reimplement that logic: it reuses both directly (see `The protect command`_ below).

Creating an Object-Lock-enabled bucket
****************************************

S3 Object Lock can only be turned on when a bucket is *created*; it cannot be enabled on an
existing bucket. If you want to use this feature with a bucket you already have, you need to
create a new bucket with Object Lock enabled and migrate to it.

MinIO
=====

Using the ``mc`` client:

.. code-block:: console

    $ mc mb --with-lock local/my-restic-repo

MinIO enables bucket versioning automatically as part of enabling Object Lock; this is
required, since Object Lock protects individual object *versions*, not object names.

AWS S3
======

Using the AWS CLI:

.. code-block:: console

    $ aws s3api create-bucket --bucket my-restic-repo --object-lock-enabled-for-bucket

As with MinIO, AWS enables versioning automatically when Object Lock is enabled for a
bucket. The same option is available as a checkbox ("Enable Object Lock") when creating a
bucket from the AWS S3 console; it is only offered at creation time.

Enabling the feature flag
****************************

This feature is gated behind the ``object-lock`` experimental feature flag (see the "Feature
flags" section of `Tuning Parameters`_ for how restic's feature flags work in general).
Enable it by setting the ``RESTIC_FEATURES`` environment variable:

.. code-block:: console

    $ export RESTIC_FEATURES=object-lock

.. _Tuning Parameters: 047_tuning_parameters.html

Without the flag enabled, ``restic protect`` refuses to run, and ``forget``/``prune`` refuse
to accept ``--skip-object-locked``, both with an error pointing at this same environment
variable.

The ``protect`` command
****************************

``restic protect`` extends S3 Object Lock retention on exactly the data the given ``--keep-*``
policy currently wants to keep -- nothing more. It reuses ``forget``'s own snapshot-selection
logic to decide which snapshots that is, and ``prune``'s used-blob/pack computation to turn
that into the exact set of packs those snapshots need to restore. This is why running
``protect`` with the same policy ``forget`` uses is safe to repeat: it will only ever
(re-)protect what the policy would currently keep, never data the policy has already decided
to discard.

.. code-block:: console

    $ restic protect --keep-daily 14 --keep-weekly 8 --keep-monthly 12 --for 120d
    Applying Policy: keep 14 daily, 8 weekly, 12 monthly snapshots
    selected 23 snapshot(s) to protect until 2026-12-06 00:00:00 (120d0h0m0s from now):
      a1b2c3d4
      ...
    protected until 2026-12-06 00:00:00: config: 1, keys: 2, index: 5, snapshots: 23, packs: 341

The objects protected are: ``config``, every file under ``keys/``, every file under
``index/``, the snapshot files the policy currently keeps, and the packs those snapshots
need. ``locks/*`` is never protected, so ``restic unlock`` continues to work normally.

The retention date restic attempts to set is always ``now + --for``. Repeated runs only ever
*extend* retention, never shorten it: if an object's current retention already covers the
newly computed date, the S3-compatible backend rejects the (redundant) attempt to shorten it,
and restic treats that rejection as a harmless no-op rather than an error. This means you can
run ``protect`` on a schedule with the same ``--for`` duration and it will keep pushing the
retention date forward without ever needing to read each object's current retention first.

Use ``--dry-run`` to see what would be protected without making any changes, and ``--json``
(the global flag) for machine-readable output.

``forget --skip-object-locked``
*********************************

Normally, ``forget`` fails outright if it cannot delete a snapshot file it selected for
removal. With ``--skip-object-locked``, a snapshot file that is still under an active Object
Lock retention (for example because an earlier, longer-running ``protect --for`` window has
not expired yet) is skipped instead of failing the whole run:

.. code-block:: console

    $ restic forget --keep-daily 14 --keep-weekly 8 --keep-monthly 12 --skip-object-locked
    Applying Policy: keep 14 daily, 8 weekly, 12 monthly snapshots
    selected for removal: 15
    removed: 11
    object locked: 4

Without ``--skip-object-locked``, ``forget``'s behavior is completely unchanged: this is
regression-tested, and the flag requires the ``object-lock`` feature flag to be set.

``prune --skip-object-locked``
********************************

The same idea applies to ``prune``: normally it fails if it cannot delete a pack it
determined is unused. With ``--skip-object-locked``, a still-locked unused pack is left in
place for a future ``prune`` run instead of failing:

.. code-block:: console

    $ restic prune --skip-object-locked
    ...
    unused packs: 93
    removed: 74
    object locked: 19

As with ``forget``, omitting the flag leaves ``prune``'s behavior byte-for-byte unchanged
(also regression-tested), and the flag requires the ``object-lock`` feature flag to be set.

Recommended daily flow
****************************

Run ``protect``, then ``forget``, then ``prune``, in that order, for example once a day:

.. code-block:: console

    $ restic protect --keep-daily 14 --keep-weekly 8 --keep-monthly 12 --for 120d
    $ restic forget  --keep-daily 14 --keep-weekly 8 --keep-monthly 12 --skip-object-locked
    $ restic prune   --skip-object-locked

``protect`` renews retention on everything the policy currently wants to keep. ``forget``
then removes whatever the policy no longer wants, skipping anything still locked from an
earlier, longer protection window. ``prune`` finally reclaims unused packs, skipping any
that are still locked. Because ``protect`` only ever re-protects what the *current* policy
would keep, a snapshot the policy has decided to discard stops being renewed and, once its
existing lock naturally expires, a later ``forget``/``prune`` run is able to remove it.

Use the exact same ``--keep-*`` policy for ``protect`` and ``forget`` -- if they differ,
``protect`` may end up protecting snapshots ``forget`` is trying to remove (or vice versa),
which defeats the purpose of this feature.

COMPLIANCE vs. GOVERNANCE mode
********************************

S3 Object Lock supports two retention modes:

- **COMPLIANCE**: nobody, including the bucket/account owner, can delete or shorten the
  retention on a locked object before it expires. This is the mode ``restic protect`` uses,
  and the primary mode this feature targets and supports at the command level.
- **GOVERNANCE**: like COMPLIANCE, but users holding a special bypass permission
  (``s3:BypassGovernanceRetention``) can delete or shorten retention early. GOVERNANCE mode
  is exercised in this project's internal tests, but no command in restic currently exposes a
  way to perform a governance bypass. If you need that capability today, it has to be done
  directly against the S3-compatible backend, outside of restic.

Current limitations
****************************

- **``s3`` backend only.** No other restic backend implements Object Lock support.
- **No GOVERNANCE bypass.** restic can create and detect GOVERNANCE-locked objects (in
  tests), but no command exposes a way to bypass a GOVERNANCE retention early.
- **Alpha / experimental.** The feature flag, the ``protect`` command, and the
  ``--skip-object-locked`` flags may all change behavior, flags, or output format in a future
  restic release, or be removed, without the usual deprecation period stable features get.
- **Object Lock can only be enabled at bucket creation time.** There is no way for restic to
  retrofit Object Lock onto an existing bucket; you need a new, Object-Lock-enabled bucket
  and to migrate your repository to it.
- **``protect`` targets COMPLIANCE mode.** It does not currently offer a way to choose
  GOVERNANCE mode for the retention it sets.
