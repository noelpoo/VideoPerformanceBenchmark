import sys
import subprocess
import datetime

from Common import *


class Benchmark(object):
    def __init__(self, result_name):
        super(Benchmark, self).__init__()
        cur_time = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        self.result_name = cur_time + "_" + result_name
        self.tmp_file_dir = tmp_dir + self.result_name
        if not os.path.exists(tmp_dir):
            os.mkdir(tmp_dir)
        if not os.path.exists(self.tmp_file_dir):
            os.mkdir(self.tmp_file_dir)

        self.csv_file = root_dir + self.result_name + '.csv'
        with open(self.csv_file, 'w') as f:
            f.write('framerate,stutter_rate\n')

    def run_with_file(self, org_mp4):
        tmp_mp4, tmp_yuv, new_width, new_height = self.crop_video(org_mp4, 860, 640)
        ts = self.get_video_frame_ts(tmp_mp4)
        ti = self.get_video_frame_ti(tmp_yuv, new_width, new_height)
        tsti = self.get_video_frame_info(ts, ti)
        fps, stutter_rate = self.get_fps_stutter(tsti)

        print('fps, sutter rate: %d, %f' % (fps, stutter_rate))
        with open(self.csv_file, 'w') as file:
            file.write('%d,%f/n' % (fps, stutter_rate))

    def get_video_frame_ts(self, video_file):
        tmp_file = tmp_dir + self.result_name + '/tmp_frame_ts.txt'
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
        command = f"{tool_ffprobe} -i {video_file} -show_frames > {tmp_file}"
        p = subprocess.Popen(command, shell=True)
        p.wait()

        ret = []
        data = get_file_data(tmp_file)
        print(f'tmp file is {tmp_file}')
        lines = data.strip().split()
        for line in lines:
            if line.startswith('pkt_pts_time'):
                ret.append(safe_cast(line.split('=')[1], float, 0.0))
        return ret

    @staticmethod
    def get_video_frame_ti(video_file, width, height):
        command = [tool_tisi, '-i', video_file, '-x', str(width), '-y', str(height),
                   '-f', '420']
        out = subprocess.check_output(command)

        ret = [0.0]
        lines = str(out.strip()).split('\\n')
        for line in lines:
            if line.startswith('TI('):
                value = line.split(" : ")[1]
                ret.append(safe_cast(value, float, 0.0))

        return ret


    @staticmethod
    def get_video_frame_info(ts, ti):
        if len(ts) != len(ti):
            print(f'len ts, len ti: {len(ts)}, {len(ti)}')
            raise Exception('len(ts) != len(ti)')

        ret = []
        len_of_frames = len(ts)
        for i in range(0, len_of_frames):
            if ti[i] > ti_upp_limit:
                ret.append((ts[i], ti[i]))
        return ret

    @staticmethod
    def get_fps_stutter(tsti):
        if len(tsti) <= 1:
            return len(tsti), 100.0

        stutter_time = 0
        total_time = tsti[-1][0] - tsti[0][0]
        len_tsti = len(tsti)
        for i in range(0, len_tsti - 1):
            gap = tsti[i + 1][0] - tsti[i][0]
            if gap >= stutter_upp_limit_time:
                stutter_time += gap

        stutter_perc = round(stutter_time / total_time * 100, 3)
        fps = int(len(tsti) / total_time)

        return fps, stutter_perc

    def crop_video(self, video_file, x=0, y=0):
        tmp0_mp4 = self.tmp_file_dir + '/tmp0.mp4'
        if os.path.exists(tmp0_mp4):
            os.remove(tmp0_mp4)
        # test duration length set as 5 seconds
        command = [tool_ffmpeg, '-ss', '00:00:00', '-t', '5', '-i', video_file,
                   '-vcodec', 'copy', '-an', tmp0_mp4]
        subprocess.check_output(command)

        command = [tool_ffprobe, '-v', 'quiet', '-print_format', 'json',
                   '-show_format', '-show_streams', video_file]
        out = subprocess.check_output(command)
        org_video_format = eval(out.strip())
        width = int(org_video_format['streams'][0]['width'])
        height = int(org_video_format['streams'][0]['height'])
        '''
        coded_width = int(org_video_format['streams'][0]['coded_width'])
        coded_height = int(org_video_format['streams'][0]['coded_height'])
        x = x if x != 0 else (width - crop_width) / 2
        y = y if y != 0 else (height - crop_height) / 2
        '''
        tmp_mp4 = self.tmp_file_dir + '/tmp.mp4'
        if os.path.exists(tmp_mp4):
            os.remove(tmp_mp4)
        command = [tool_ffmpeg, '-i', tmp0_mp4, '-strict', '-2', '-vf',
                   'crop=%d:%d:%d:%d' % (int(width/2), int(height/2), int(width/2), int(height/2)),
                   tmp_mp4]
        subprocess.check_output(command)

        new_width = int(width/2)
        new_height = int(height/2)

        tmp_yuv = self.tmp_file_dir + '/tmp.yuv'
        if os.path.exists(tmp_yuv):
            os.remove(tmp_yuv)
        command = [tool_ffmpeg, '-i', tmp_mp4, tmp_yuv]
        subprocess.check_output(command)

        return tmp_mp4, tmp_yuv, new_width, new_height


def main():
    if len(sys.argv) < 3:
        print('\ninput correct params:')
        print('python benchmark_for_file video_file result_name\n')
        exit(-1)

    test_video_file = sys.argv[1]
    test_result_name = sys.argv[2]

    sys.path.append([tool_dir_ffmpeg, tool_dir_tisi])
    add_tool_permission()

    benchmark = Benchmark(test_result_name)
    benchmark.run_with_file(test_video_file)


if __name__ == '__main__':
    main()
