import matplotlib.pyplot as plt
import sys
import subprocess
import datetime

from Common import *


class VideoTiSi(object):
    def __init__(self, result_name):
        super(VideoTiSi, self).__init__()
        cur_time = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        self.result_name = cur_time + '_' + result_name
        self.tmp_file_dir = tmp_dir + self.result_name
        if not os.path.exists(tmp_dir):
            os.mkdir(tmp_dir)
        if not os.path.exists(self.tmp_file_dir):
            os.mkdir(self.tmp_file_dir)
        if not os.path.exists(tisi_data_dir):
            os.mkdir(tisi_data_dir)
        self.csv_file = tisi_data_dir + self.result_name + '.csv'
        with open(self.csv_file, 'w') as f:
            f.write('ts,ti,si\n')

    def run_tisi_data(self, org_mp4):
        tmp_mp4, tmp_yuv, new_width, new_height = self.crop_video(org_mp4)
        ts = self.get_video_ts(tmp_mp4)
        ti, si = self.get_video_ti_si(tmp_yuv, new_width, new_height)
        if len(ts) != len(ti):
            raise Exception('len(ts) != len(ti)')
        elif len(ts) != len(si):
            raise Exception('len(ts) != len(si)')
        for i in range(0, len(ts)):
            with open(self.csv_file, 'a+') as f:
                f.write('%f,%f,%f\n' % (ts[i], ti[i], si[i]))

        plt.subplot(2, 1, 1)
        plt.title('TI_' + self.result_name)
        plt.axhline(4, color='red', linestyle='--')
        plt.scatter(ts, ti, s=1.5)

        plt.subplot(2, 1, 2)
        plt.title('SI_' + self.result_name)
        plt.plot(ts, si, color = 'orange')
        plt.show()

    def get_video_ts(self, file):
        tmp_file = self.tmp_file_dir + '/tmp_file_ts.txt'
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
        # command = [tool_ffprobe, '-i', file, '-show_frames', '>', tmp_file]
        command = f'{tool_ffprobe} -i {file} -show_frames > {tmp_file}'
        p = subprocess.Popen(command, shell=True)
        p.wait()

        ret = []
        data = get_file_data(tmp_file)
        print(f'this is the tmp_file: {tmp_file}')
        lines = data.strip().split()
        for line in lines:
            if line.startswith('pkt_pts_time'):
                ts_val = safe_cast(line.split('=')[1], float, 0.0)
                ret.append(ts_val)

        return ret

    @staticmethod
    def get_video_ti_si(yuv, width, height):
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

    def crop_video(self, video_file):
        tmp0_mp4 = self.tmp_file_dir + '/tmp0.mp4'
        if os.path.exists(tmp0_mp4):
            os.remove(tmp0_mp4)
        # test duration length set as 5 seconds
        command = [tool_ffmpeg, '-ss', '00:00:00', '-t', '60', '-i', video_file,
                   '-vcodec', 'copy', '-an', tmp0_mp4]
        subprocess.check_output(command)

        command = [tool_ffprobe, '-v', 'quiet', '-print_format', 'json',
                   '-show_format', '-show_streams', video_file]
        out = subprocess.check_output(command)
        org_video_format = eval(out.strip())
        width = int(org_video_format['streams'][0]['width'])
        height = int(org_video_format['streams'][0]['height'])
        new_width, width_start = int(width / 2), int(width / 2)
        new_height, height_start = int(height / 2), int(width / 2)
        tmp_mp4 = self.tmp_file_dir + '/tmp.mp4'
        if os.path.exists(tmp_mp4):
            os.remove(tmp_mp4)
        command = [tool_ffmpeg, '-i', tmp0_mp4, '-strict', '-2', '-vf',
                   'crop=%d:%d:%d:%d' % (new_width, new_height, width_start, height_start),
                   tmp_mp4]
        subprocess.check_output(command)

        new_width = int(width / 2)
        new_height = int(height / 2)

        tmp_yuv = self.tmp_file_dir + '/tmp.yuv'
        if os.path.exists(tmp_yuv):
            os.remove(tmp_yuv)
        command = [tool_ffmpeg, '-i', tmp_mp4, tmp_yuv]
        subprocess.check_output(command)

        return tmp_mp4, tmp_yuv, new_width, new_height


def main():
    if len(sys.argv) < 3:
        print('\ninput correct params:')
        print('python Get_video_TI_SI.py video_file result_name\n')
        exit(-1)

    test_video_file = sys.argv[1]
    test_result_name = sys.argv[2]

    sys.path.append([tool_dir_ffmpeg, tool_dir_tisi])
    add_tool_permission()

    get_video_ti_si = VideoTiSi(test_result_name)
    get_video_ti_si.run_tisi_data(test_video_file)


if __name__ == '__main__':
    main()
