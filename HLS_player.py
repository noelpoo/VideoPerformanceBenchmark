import asyncio
import json
from asyncio import CancelledError
from datetime import datetime

import aiohttp
import click
import m3u8
import time


def timestamp_to_datetime_str(timestamp):
    datetime_str = datetime.fromtimestamp(timestamp / 1000.0).strftime('%Y_%m_%d %H.%M.%S')
    return datetime_str + '.{:03d}'.format(int((timestamp / 1000.0 - int(timestamp / 1000)) * 1000))


class PlayerState:
    Init = 0
    Playing = 1
    Stalling = 2
    Stopped = 3


class HlsPlayer:

    def __init__(self, hls_url, duration, event_loop):
        self.hls_url = hls_url
        self.duration = duration
        self.loop = event_loop
        self.queue = asyncio.Queue(maxsize=10, loop=self.loop)
        self.ts_consume_cond = asyncio.Condition()
        self.is_playing_startup = False
        self.is_downloading_startup = False
        self.playing_start_time = 0
        self.downloading_start_time = 0
        self.start_timestamp = 0
        self.start_loop_time = 0
        self.last_downloaded_ts_url = None
        self.last_played_ts_url = None
        self.is_playing_exceeds_duration = False
        self.stats_ts = []
        self.stats_play = []
        self.stats_m3u8 = []
        self._state = PlayerState.Init

    async def ts_download(self, http_session: aiohttp.ClientSession):
        init_segments_count = 0
        try:
            while True:
                if self.is_playing_exceeds_duration:
                    print('Playing time exceeds duration {}s, exits downloading'.format(self.duration))
                    return
                segments_for_downloading = []
                if not self.is_downloading_startup:
                    if self.downloading_start_time == 0:
                        print('Downloading starts up')
                        self.downloading_start_time = int(self.loop.time() * 1000)
                    else:
                        print('Downloading reset')
                    try:
                        print('Fetch m3u8')
                        m3u8_start_time = int(self.loop.time() * 1000)
                        m3u8_resp = await http_session.get(self.hls_url, timeout=10)
                        if m3u8_resp.status != 200:
                            print('Download m3u8 failed')
                            continue
                        m3u8_content = await m3u8_resp.text()
                        m3u8_end_time = int(self.loop.time() * 1000)
                        self.stats_m3u8.append({
                            'start_time': m3u8_start_time,
                            'end_time': m3u8_end_time,
                            'start_ts': self.get_ts(m3u8_start_time),
                            'end_ts': self.get_ts(m3u8_end_time),
                            'size': len(m3u8_content)
                        })
                    except Exception as e:
                        print('Download m3u8 failed due to {}'.format(e))
                        continue

                    m3u8_obj = m3u8.loads(m3u8_content, uri=self.hls_url)
                    if len(m3u8_obj.segments) == 0:
                        print('Illegal m3u8, empty segments')
                        continue
                    if m3u8_obj.start.time_offset < -0.1:
                        print('Start downloading from time offset {}'.format(m3u8_obj.start.time_offset))
                        time_offset = m3u8_obj.start.time_offset
                        for s in m3u8_obj.segments[::-1]:
                            if time_offset + s.duration < 0.1:
                                segments_for_downloading.insert(0, s)
                                time_offset += s.duration
                    elif m3u8_obj.start.time_offset > 0.1:
                        print('Start downloading from time offset {}'.format(m3u8_obj.start.time_offset))
                        time_offset = m3u8_obj.start.time_offset
                        for s in m3u8_obj.segments:
                            if time_offset - s.duration > -0.1:
                                segments_for_downloading.append(s)
                                time_offset -= s.duration
                    else:
                        segments_for_downloading = m3u8_obj.segments
                    init_segments_count = min(len(segments_for_downloading), self.queue.maxsize)
                    print('Init segments count {}'.format(init_segments_count))
                    self.is_downloading_startup = True
                else:
                    print('Buffered ts files queue size: {}'.format(self.queue.qsize()))
                    if self.queue.qsize() >= init_segments_count - 1 and self.queue.qsize() != 0:
                        async with self.ts_consume_cond:
                            print('Downloader wait for ts_consume_cond ready')
                            await self.ts_consume_cond.wait()
                    m3u8_failed_times = 0
                    while len(segments_for_downloading) == 0:
                        try:
                            print('Fetch m3u8')
                            m3u8_start_time = int(self.loop.time() * 1000)
                            m3u8_resp = await http_session.get(self.hls_url, timeout=10)
                            if m3u8_resp.status != 200:
                                print('Download m3u8 failed')
                                m3u8_failed_times += 1
                                if m3u8_failed_times == 10:
                                    print('Exceeds max m3u8 downloading failed times!')
                                    return
                                await asyncio.sleep(0.5)
                                continue
                            m3u8_content = await m3u8_resp.text()
                            m3u8_end_time = int(self.loop.time() * 1000)
                            self.stats_m3u8.append({
                                'start_time': m3u8_start_time,
                                'end_time': m3u8_end_time,
                                'start_ts': self.get_ts(m3u8_start_time),
                                'end_ts': self.get_ts(m3u8_end_time),
                                'size': len(m3u8_content)
                            })
                        except Exception as e:
                            print('Download m3u8 failed due to {}'.format(e))
                            m3u8_failed_times += 1
                            if m3u8_failed_times == 10:
                                print('Exceeds max m3u8 downloading failed times!')
                                return
                            await asyncio.sleep(0.5)
                            continue

                        m3u8_obj = m3u8.loads(m3u8_content, uri=self.hls_url)
                        is_last_downloaded_ts_in = False
                        for index, segment in enumerate(m3u8_obj.segments):
                            if self.last_downloaded_ts_url is None:
                                raise Exception('Downloading has been started up but nothing is downloaded yet')
                            elif self.last_downloaded_ts_url == segment.base_uri + segment.uri:
                                is_last_downloaded_ts_in = True
                                if index == len(m3u8_obj.segments) - 1:
                                    print('m3u8 content is not refreshed')
                                    await asyncio.sleep(1)
                                else:
                                    segments_for_downloading += m3u8_obj.segments[index + 1:]
                                    break
                        if not is_last_downloaded_ts_in:
                            segments_for_downloading += m3u8_obj.segments
                if len(segments_for_downloading) == 0:
                    continue
                print('Will download {} ts files'.format(len(segments_for_downloading)))
                for index, segment in enumerate(segments_for_downloading):
                    downloading_url = segment.base_uri + segment.uri
                    downloading_start_time = int(self.loop.time() * 1000)
                    segment_uri_path = segment.uri
                    if segment_uri_path.find('?') > 0:
                        segment_uri_path = segment_uri_path.split('?')[0]
                    print('Start downloading {} at {}'.format(segment_uri_path,
                                                              downloading_start_time))
                    self.last_downloaded_ts_url = downloading_url
                    try:
                        resp = await http_session.get(downloading_url, timeout=15)
                        if resp.status != 200:
                            print('Failed downloading {}'.format(
                                segment_uri_path
                            ))
                            self.stats_ts.append({
                                'full_url': downloading_url,
                                'uri': segment_uri_path,
                                'is_succeed': False,
                                'error': str(resp.status),
                                'duration': segment.duration,
                                'start_time': downloading_start_time,
                                'end_time': int(self.loop.time() * 1000),
                                'start_ts': self.get_ts(downloading_start_time),
                                'end_ts': self.get_ts(int(self.loop.time() * 1000))
                            })
                            continue
                        download_resp_back_time = int(self.loop.time() * 1000)
                        resp_content = await resp.read()
                    except asyncio.TimeoutError:
                        print('Failed downloading {} due to timeout'.format(
                            segment_uri_path
                        ))
                        self.stats_ts.append({
                            'full_url': downloading_url,
                            'uri': segment_uri_path,
                            'is_succeed': False,
                            'error': 'Timeout',
                            'duration': segment.duration,
                            'start_time': downloading_start_time,
                            'end_time': int(self.loop.time() * 1000),
                            'start_ts': self.get_ts(downloading_start_time),
                            'end_ts': self.get_ts(int(self.loop.time() * 1000))
                        })
                        continue
                    except Exception as e:
                        print('Failed downloading {} due to exception, {}'.format(
                            segment_uri_path, e
                        ))
                        self.stats_ts.append({
                            'full_url': downloading_url,
                            'uri': segment_uri_path,
                            'is_succeed': False,
                            'error': '{}'.format(e),
                            'duration': segment.duration,
                            'start_time': downloading_start_time,
                            'end_time': int(self.loop.time() * 1000),
                            'start_ts': self.get_ts(downloading_start_time),
                            'end_ts': self.get_ts(int(self.loop.time() * 1000))
                        })
                        continue

                    downloading_finish_time = int(self.loop.time() * 1000)
                    print('Finish downloading {} with duration {}ms at {}'.format(
                        segment_uri_path,
                        downloading_finish_time - downloading_start_time,
                        downloading_finish_time))
                    payload = {
                        'full_url': downloading_url,
                        'uri': segment_uri_path,
                        'content': resp_content,
                        'size': len(resp_content),
                        'latency': download_resp_back_time - downloading_start_time,
                        'content_download_time': downloading_finish_time - download_resp_back_time,
                        'download_time': downloading_finish_time - downloading_start_time,
                        'speed': len(resp_content) * 1000 / (downloading_finish_time - download_resp_back_time),
                        'duration': segment.duration
                    }
                    print('ts info size {:.1f}KB, latency {}ms, speed {:.1f}KB/s, total downloading time {}ms'.format(
                        payload.get('size') / 1000.0,
                        payload.get('latency'),
                        payload.get('speed') / 1000.0,
                        payload.get('download_time')))
                    self.stats_ts.append({
                        'full_url': downloading_url,
                        'uri': segment_uri_path,
                        'is_succeed': True,
                        'size': len(resp_content),
                        'latency': download_resp_back_time - downloading_start_time,
                        'content_download_time': downloading_finish_time - download_resp_back_time,
                        'download_time': downloading_finish_time - downloading_start_time,
                        'speed': len(resp_content) * 1000 / (downloading_finish_time - download_resp_back_time),
                        'duration': segment.duration,
                        'start_time': downloading_start_time,
                        'end_time': downloading_finish_time,
                        'start_ts': self.get_ts(downloading_start_time),
                        'end_ts': self.get_ts(downloading_finish_time)
                    })
                    await self.queue.put(payload)
        except CancelledError:
            print('Cancel download task')
        finally:
            print('Download task exits')

    async def play(self):
        is_stall = False
        stall_start_time = 0
        stall_event = None
        play_event = None
        try:
            while True:
                if not self.is_playing_startup:
                    if self.queue.empty():
                        await asyncio.sleep(0.01)
                        continue
                    self.is_playing_startup = True
                    self.playing_start_time = int(self.loop.time() * 1000)
                    self._state = PlayerState.Playing
                    is_stall = False
                    self.stats_play.append({
                        'type': 'startup',
                        'start_time': self.downloading_start_time,
                        'end_time': self.playing_start_time,
                        'start_ts': self.get_ts(self.downloading_start_time),
                        'end_ts': self.get_ts(self.playing_start_time),
                        'duration': self.playing_start_time - self.downloading_start_time
                    })
                    print('Play starts up, duration {}ms'.format(
                        self.playing_start_time - self.downloading_start_time
                    ))
                else:
                    if self.loop.time() * 1000 > self.playing_start_time + self.duration * 1000:
                        self.is_playing_exceeds_duration = True
                        if is_stall:
                            stall_end_time = int(self.loop.time() * 1000)
                            stall_event.update({
                                'end_time': stall_end_time,
                                'duration': stall_end_time - stall_event['start_time'],
                                'end_ts': self.get_ts(stall_end_time)
                            })
                            self.stats_play.append(stall_event)
                            print('Stall ends at {}, duration {}ms'.format(stall_end_time,
                                                                           stall_end_time - stall_start_time))
                        if play_event is not None:
                            play_event.update({
                                'end_time': int(self.loop.time() * 1000),
                                'duration': int(self.loop.time() * 1000) - play_event['start_time'],
                                'end_ts': self.get_ts(int(self.loop.time() * 1000))
                            })
                            self.stats_play.append(play_event)
                        if not self.queue.empty():
                            await self.queue.get()
                        print('Playing time exceeds duration {}s, exits playing'.format(self.duration))
                        return

                if self.queue.empty():
                    if not is_stall:
                        stall_start_time = int(self.loop.time() * 1000)
                        stall_event = {
                            'type': 'stall',
                            'start_time': stall_start_time,
                            'start_ts': self.get_ts(stall_start_time)
                        }
                        if play_event is not None:
                            play_event.update({
                                'end_time': stall_start_time,
                                'duration': stall_start_time - play_event['start_time'],
                                'end_ts': self.get_ts(stall_start_time)
                            })
                            self.stats_play.append(play_event)
                            play_event = None
                        print('Stall starts at {}'.format(stall_start_time))
                    is_stall = True
                    self._state = PlayerState.Stalling
                    await asyncio.sleep(0.01)
                    continue
                if is_stall:
                    stall_end_time = int(self.loop.time() * 1000)
                    stall_event.update({
                        'end_time': stall_end_time,
                        'duration': stall_end_time - stall_start_time,
                        'end_ts': self.get_ts(stall_end_time)
                    })
                    self.stats_play.append(stall_event)
                    stall_event = None
                    is_stall = False
                    self._state = PlayerState.Playing
                    print('Stall ends at {}, duration {}ms'.format(stall_end_time,
                                                                   stall_end_time - stall_start_time))

                ts_play_start_time = int(self.loop.time() * 1000)
                item = await self.queue.get()
                if item is None:
                    if play_event is not None:
                        play_event.update({
                            'end_time': int(self.loop.time() * 1000),
                            'duration': int(self.loop.time() * 1000) - play_event['start_time'],
                            'end_ts': self.get_ts(int(self.loop.time() * 1000))
                        })
                        self.stats_play.append(play_event)
                    print('Exits playing')
                    return
                print('Play ts {} for {}s at {}'.format(
                    item.get('uri'),
                    item.get('duration'),
                    ts_play_start_time))
                if play_event is None:
                    play_event = {
                        'type': 'play',
                        'start_time': ts_play_start_time,
                        'start_ts': self.get_ts(ts_play_start_time)
                    }
                while self.loop.time() * 1000 < item.get('duration') * 1000 + ts_play_start_time:
                    await asyncio.sleep(0.01)
                self.last_played_ts_url = item.get('uri')

                async with self.ts_consume_cond:
                    print('Player notify ts_consume_cond is ready')
                    self.ts_consume_cond.notify_all()
        except CancelledError:
            print('Cancel play task')
            if is_stall:
                stall_event.update({
                    'end_time': int(self.loop.time() * 1000),
                    'duration': int(self.loop.time() * 1000) - stall_event['start_time'],
                    'end_ts': self.get_ts(int(self.loop.time() * 1000))
                })
                self.stats_play.append(stall_event)
            if play_event is not None:
                play_event.update({
                    'end_time': int(self.loop.time() * 1000),
                    'duration': int(self.loop.time() * 1000) - play_event['start_time'],
                    'end_ts': self.get_ts(int(self.loop.time() * 1000))
                })
                self.stats_play.append(play_event)
        finally:
            self._state = PlayerState.Stopped
            print('Play task exits')

    async def run(self):
        self.start_timestamp = int(time.time() * 1000)
        self.start_loop_time = int(self.loop.time() * 1000)
        async with aiohttp.ClientSession() as session:
            download_task = self.loop.create_task(self.ts_download(session))
            play_task = self.loop.create_task(self.play())
            _, pending = await asyncio.wait({download_task, play_task}, timeout=self.duration + 10,
                                            return_when=asyncio.ALL_COMPLETED)
            if download_task in pending:
                print('download task not finished')
                download_task.cancel()
            if play_task in pending:
                print('play task not finished')
                play_task.cancel()

    def get_ts(self, loop_time):
        return self.start_timestamp + loop_time - self.start_loop_time

    def export_stats_to_csv(self, csv_file_path):
        try:
            with open(csv_file_path, 'w+') as csv_file:
                play_info_header = ','.join([
                    'url',
                    'Total_Duration(s)',
                    'Last played TS'
                ])
                play_info_full_text = 'Play info\n' + play_info_header + '\n'
                play_info_text = ','.join([
                    '{}'.format(self.hls_url),
                    '{}'.format(self.duration),
                    self.last_played_ts_url
                ])
                play_info_full_text += play_info_text + '\n'
                csv_file.write(play_info_full_text)

                play_evt_header = ','.join([
                    'Event',
                    'Duration(s)',
                    'Start_Time(ms)',
                    'End_Time(ms)'
                ])
                play_evt_full_text = 'Play Events\n' + play_evt_header + '\n'
                for play_evt in self.stats_play:
                    play_evt_text = ','.join([
                        play_evt.get('type', ''),
                        '{:.2f}'.format(play_evt.get('duration', 0) / 1000.0),
                        '{}'.format(play_evt.get('start_time', 0) - self.downloading_start_time),
                        '{}'.format(play_evt.get('end_time', 0) - self.downloading_start_time)
                    ])
                    play_evt_full_text += play_evt_text + '\n'
                csv_file.write(play_evt_full_text)

                ts_evt_header = ','.join([
                    'Start_Time(ms)',
                    'TS_Name',
                    'Error',
                    'TTFB(s)',
                    'Download_Time(s)',
                    'Size(KB)',
                    'Speed(KB/s)',
                    'TS_Duration(s)'
                ])
                ts_evt_full_text = '\nTS downloads\n' + ts_evt_header + '\n'
                for ts_evt in self.stats_ts:
                    ts_evt_fields = ['{}'.format(ts_evt.get('start_time', 0) - self.downloading_start_time),
                                     ts_evt.get('uri', '')]
                    if ts_evt.get('is_succeed', True):
                        ts_evt_fields.append('')
                        ts_evt_fields.append('{:.2f}'.format(ts_evt.get('latency', 0) / 1000.0))
                        ts_evt_fields.append('{:.2f}'.format(ts_evt.get('download_time', 0) / 1000.0))
                        ts_evt_fields.append('{:.1f}'.format(ts_evt.get('size', 0) / 1024.0))
                        ts_evt_fields.append('{:.1f}'.format(ts_evt.get('speed', 0) / 1024.0))
                    else:
                        ts_evt_fields.append(ts_evt.get('error', ''))
                        for _ in range(4):
                            ts_evt_fields.append('')
                    ts_evt_fields.append('{:.2f}'.format(ts_evt.get('duration', 0)))
                    ts_evt_text = ','.join(ts_evt_fields)
                    ts_evt_full_text += ts_evt_text + '\n'
                csv_file.write(ts_evt_full_text)
        except Exception as e:
            print('Error happens when exporting stats to csv, {}'.format(e))
            return False
        return True

    @property
    def state(self):
        return self._state


def main(url, duration, need_json_report):
    loop = asyncio.get_event_loop()
    player = HlsPlayer(url, duration, loop)
    loop.run_until_complete(player.run())
    loop.close()
    if need_json_report or not player.export_stats_to_csv('hls_play_{}.csv'.format(timestamp_to_datetime_str(
            int(time.time() * 1000)))):
        with open('play_event_{}.json'.format(
                timestamp_to_datetime_str(int(time.time() * 1000))), 'w+') as play_events_json_file:
            json.dump(player.stats_play, play_events_json_file)
        with open('ts_downloads_{}.json'.format(
                timestamp_to_datetime_str(int(time.time() * 1000))), 'w+') as ts_downloads_json_file:
            json.dump(player.stats_ts, ts_downloads_json_file)


@click.command()
@click.option("--url", type=str,
              default='',
              help="The hls url")
@click.option("--duration", type=int,
              default=60,
              help="The max playing duration. Unit: second. Default: 60")
@click.option("--json-report", 'need_json_report', is_flag=True, default=False,
              help="Save json report")
def command(url, duration, need_json_report):
    main(url, duration, need_json_report)


if __name__ == '__main__':
    command()
