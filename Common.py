import os
import stat
import platform
import logging


root_dir = os.path.split(os.path.abspath(__file__))[0] + '/'

tools_dir = ''
tool_xrecord = ''
sys_str = platform.system()
if sys_str == "Windows":
    tools_dir = root_dir + 'tools/win/'
elif sys_str == "Darwin":
    tools_dir = root_dir + 'tools/mac/'
elif sys_str == "Linux":
    tools_dir = root_dir + 'tools/linux/'

if sys_str == "Linux":
    tool_ffmpeg = 'ffmpeg'
    tool_ffplay = 'ffplay'
    tool_ffprobe = 'ffprobe'
    tool_tisi = tools_dir + 'tisi/TISI'
else:
    tool_ffmpeg = tools_dir + 'ffmpeg/ffmpeg'
    tool_ffplay = tools_dir + 'ffmpeg/ffplay'
    tool_ffprobe = tools_dir + 'ffmpeg/ffprobe'
    tool_tisi = tools_dir + 'tisi/TISI'

tool_dir_ffmpeg = tools_dir + 'ffmpeg'
tool_dir_tisi = tools_dir + 'tisi'

tmp_dir = root_dir + 'tmp/'

#cropping parameters
crop_width = 280
crop_height = 400

stutter_upp_limit_time = 0.2
test_data_count = 5


def add_tool_permission():
    if sys_str == 'Darwin' or sys_str == 'Linux':
        os.chmod(tool_ffmpeg, stat.S_IRWXU)
        os.chmod(tool_ffplay, stat.S_IRWXU)
        os.chmod(tool_ffprobe, stat.S_IRWXU)
        os.chmod(tool_tisi, stat.S_IRWXU)


def safe_cast(val, to_type, default=None):
    try:
        return to_type(val)
    except (ValueError, TypeError):
        return default


def get_file_data(file_path):
    try:
        data = None
        f = open(file_path, 'r')
        if f is not None:
            data = f.read()
            f.close()

        if data is not None:
            return data.strip()
        else:
            return None
    except Exception as e:
        logging.error(e, exc_info=True)
        return None





