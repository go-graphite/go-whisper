#!/usr/bin/env python3
"""Check exported classic files with Python Whisper (pip install whisper==1.1.10)."""
import argparse
import json
import math
from pathlib import Path
import struct
import whisper


def latest_timestamp(path, archives):
    latest = 0
    with open(path, 'rb') as stream:
        for archive in archives:
            stream.seek(archive['offset'])
            data = stream.read(archive['points'] * 12)
            if len(data) != archive['points'] * 12:
                raise ValueError(f'truncated archive: {path}')
            for offset in range(0, len(data), 12):
                latest = max(latest, struct.unpack_from('>I', data, offset)[0])
    return latest


def same_value(a, b):
    if a is None or b is None:
        return a is b
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    return struct.pack('>d', a) == struct.pack('>d', b)


def compare(source, exported, now_override):
    old, new = whisper.info(source), whisper.info(exported)
    for key in ('aggregationMethod', 'maxRetention', 'xFilesFactor'):
        if old[key] != new[key]:
            raise AssertionError(f'{source}: {key}: {old[key]} != {new[key]}')
    old_retentions = [(a['secondsPerPoint'], a['points']) for a in old['archives']]
    new_retentions = [(a['secondsPerPoint'], a['points']) for a in new['archives']]
    if old_retentions != new_retentions:
        raise AssertionError(f'{source}: retention mismatch')
    now = now_override or latest_timestamp(source, old['archives']) + 1
    queries = 0
    compared = 0
    present = 0
    for archive in old['archives']:
        step, retention = archive['secondsPerPoint'], archive['retention']
        for start, end in [(now-retention, now), (now-min(retention, 100*step), now),
                           (now-min(retention, 10*step)+1, now-step), (now-step, now-step)]:
            expected = whisper.fetch(source, start, end, now=now, archiveToSelect=step)
            actual = whisper.fetch(exported, start, end, now=now, archiveToSelect=step)
            if (expected is None) != (actual is None):
                raise AssertionError(f'{source}: missing result for {start}:{end}:{step}')
            if expected is not None:
                if expected[0] != actual[0] or len(expected[1]) != len(actual[1]):
                    raise AssertionError(f'{source}: time-grid mismatch for {start}:{end}:{step}')
                for i, (a, b) in enumerate(zip(expected[1], actual[1])):
                    if not same_value(a, b):
                        raise AssertionError(f'{source}: mismatch at {expected[0][0]+i*step}: {a} != {b}')
                    present += a is not None and not math.isnan(a)
                compared += len(expected[1])
            queries += 1
    return {'source': str(source), 'exported': str(exported), 'now': now,
            'queries': queries, 'values_checked': compared, 'present_values_checked': present}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pair', nargs=2, action='append', required=True,
                        metavar=('SOURCE_WSP', 'EXPORTED_WSP'))
    parser.add_argument('--now', type=int, default=0,
                        help='Fixed clock; default is newest source timestamp + 1.')
    args = parser.parse_args()
    print(json.dumps([compare(Path(a), Path(b), args.now) for a, b in args.pair], indent=2))


if __name__ == '__main__':
    main()
