import os
import click
import numpy as np
import wave
import time
import datetime
import subprocess
import asyncio

from Common import *
from Get_video_TI_SI import VideoTiSi


class AvSync(object):
    def __init__(self, result_name):
        super(AvSync, self).__init__()
        cur_time = datetime.datetime.now('%Y-%m-%d-%H-%M-%S')
        self.result_name = cur_time + '_' + result_name
        self.tmp_dir = tmp_dir + self.result_name
        self.wav_file_dir = tmp_dir + cur_time + '_wav.wav'
        if not os.path.exists(tmp_dir):
            os.mkdir(tmp_dir)
        if not os.path.exists(self.tmp_dir):
            os.mkdir(self.tmp_dir)
        if not os.path.exists(tisi_data_dir):
            os.mkdir(tisi_data_dir)
        self.csv_file = tisi_data_dir + self.result_name + '.csv'
        with open(self.csv_file, 'w') as f:
            f.write('ts,ti,si\n')

    def get_lit_frames(self, ti_data):

    async def get_video_ti_si(yuv, width, height):
        command = [tool_tisi, '-i', yuv, '-x', str(width), '-y', str(height), '-f', '420']
        out = subprocess.check_output(command)
        lines = str(out.strip()).split('\\n')

        ret_ti = [0.0]
        ret_si = []
        for line in lines:
            if line.startswith('SI('):
                si_val = safe_cast(line.split(' : ')[1], float, 0.0)
                ret_si.append(si_val)
            elif line.startswith('TI('):
                ti_val = safe_cast(line.split(' : ')[1], float, 0.0)
                ret_ti.append(ti_val)

        return ret_ti, ret_si

    async def crop_video(self, video_file):
        tmp0_mp4 = self.tmp_file_dir + '/tmp0.mp4'
        if os.path.exists(tmp0_mp4):
            os.remove(tmp0_mp4)
        # test duration length set as 10 seconds
        command = [tool_ffmpeg, '-ss', '00:00:00', '-t', '10', '-i', video_file,
                   '-vcodec', 'copy', '-an', tmp0_mp4]
        subprocess.check_output(command)

        command = [tool_ffprobe, '-v', 'quiet', '-print_format', 'json',
                   '-show_format', '-show_streams', video_file]
        out = subprocess.check_output(command)
        org_video_format = eval(out.strip())
        width = int(org_video_format['streams'][0]['width'])
        height = int(org_video_format['streams'][0]['height'])
        # crop 1/10 of the frame in the centre most position
        new_width, width_start = int(width / 10), int(width / 2 - width / (10 * 2))
        new_height, height_start = int(height / 10), int(width / 2 - width / (10 * 2))
        tmp_mp4 = self.tmp_file_dir + '/tmp.mp4'
        if os.path.exists(tmp_mp4):
            os.remove(tmp_mp4)
        command = [tool_ffmpeg, '-i', tmp0_mp4, '-strict', '-2', '-vf',
                   'crop=%d:%d:%d:%d' % (new_width, new_height, width_start, height_start),
                   tmp_mp4]
        subprocess.check_output(command)
        tmp_yuv = self.tmp_file_dir + '/tmp.yuv'
        if os.path.exists(tmp_yuv):
            os.remove(tmp_yuv)
        command = [tool_ffmpeg, '-i', tmp_mp4, tmp_yuv]
        subprocess.check_output(command)

        return tmp_mp4, tmp_yuv, new_width, new_height

    async def get_video_frame_ts(self, video_file):
        tmp_file = tmp_dir + self.result_name + '/tmp_frame_ts.txt'
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
        command = f"{tool_ffprobe} -i {video_file} -show_frames > {tmp_file}"
        p = subprocess.Popen(command, shell=True)
        p.wait()

        ret = []
        data = get_file_data(tmp_file)
        # print(f'tmp file is {tmp_file}')
        lines = data.strip().split()
        for line in lines:
            if line.startswith('pkt_pts_time'):
                ret.append(safe_cast(line.split('=')[1], float, 0.0))
        return ret
