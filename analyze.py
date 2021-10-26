#!/usr/bin/env python
# coding: utf-8

# In[9]:


import re
import time
import pickle
from threading import Thread
import sys
import chess
import chess.engine
import argparse
from chess import QUEEN
from collections import defaultdict

#In[11]:
intervals = (
    ('weeks', 604800),  # 60 * 60 * 24 * 7
    ('days', 86400),    # 60 * 60 * 24
    ('hours', 3600),    # 60 * 60
    ('minutes', 60),
    ('seconds', 1),
    )
def display_time(seconds, granularity=2):
    result = []

    for name, count in intervals:
        value = seconds // count
        if value:
            seconds -= value * count
            if value == 1:
                name = name.rstrip('s')
            result.append("{} {}".format(round(value), name))
    return ', '.join(result[:granularity])

def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', status))
    sys.stdout.flush()


# In[12]:
data = []
save_file = "uniform_analyzed_backup.pickle"
last_starting_line = 0
save_thread = None
last_save = 0

def save_data(new_data, line_no):
    global last_starting_line
    global data
    global last_save
    try:
        data.append(new_data)
        last_starting_line = line_no
        if len(data)-last_save>=15: # save for each 15 game
            with open(save_file, "ab") as s_file:
                pickle.dump(data[-(len(data)-last_save):], s_file)
            last_save = len(data)
            with open("bookmarkk.txt", "w") as file:
                file.write("Bookmark: ("+str(last_starting_line)+","+str(data[-1]["EventNo"]+1)+")")
    except Exception as e:
        print("\nAn unhandled exception occurred: {}".format(type(e)))
        raise e
    return
    

def main(threads=8, counter=1, bookmark=0, last_game = 1000056, engine_path="stockfish_13_win_x64_bmi2\\stockfish_13_win_x64_bmi2.exe"):
    global last_starting_line
    global save_thread
    
    engine = chess.engine.SimpleEngine.popen_uci(engine_path)
    engine.configure({"Threads": threads})

    last_starting_line = 0
    start_time = time.time()
    speed_timer = time.time()
    speed = [0, [0,0,0,0,0]]

    with open("uniform_uci.pgn") as file:
        try:
            first_loop = True
            new_data = {"EventNo":0, "WhiteElo":0, "BlackElo":0, "PGN": "", "MoveScore": [], "MoveScoreCP": [], "BestMove": [], "BestMoveScores":[], "ValidMoveCount":[], "GameFeatures":{}, "Result": ""}
            pgn = False
            for line_no, line in enumerate(file):
                if line_no < bookmark:
                    continue
                line = line.strip()
                if "Event" in line:
                    if not first_loop:
                        ans = re.search(" .{3,7}$",new_data["PGN"])
                        if not ans:
                            new_data = {"EventNo":0, "WhiteElo":0, "BlackElo":0, "PGN": "", "MoveScore": [], "MoveScoreCP": [], "BestMove": [], "BestMoveScores":[], "ValidMoveCount":[], "GameFeatures":{}, "Result": ""}                            
                            new_data["EventNo"] = counter
                            if counter>=last_game:
                                break
                            counter += 1
                            continue
                        new_data["PGN"] = new_data["PGN"][:ans.start()]
                        new_data["Result"] = ans.group()
                        if not ( len(new_data["PGN"])<45 or new_data["WhiteElo"]==-1): #if too short or no ELO available pass
                            board = chess.Board()
                            move_list = new_data["PGN"].split(" ")

                            first_check = True
                            first_queen_move = True
                            features = defaultdict(int)
                            for i, e in enumerate(move_list):
                                move = None
                                try:
                                    move = board.push_san(e)
                                    info = engine.analyse(board, chess.engine.Limit(depth=8), multipv=5)
                                    new_data["MoveScore"].append(info[0]["score"].wdl().white().expectation())
                                    new_data["MoveScoreCP"].append(info[0]["score"].white().score())
                                    moved_piece = board.piece_type_at(move.from_square)
                                    captured_piece = board.piece_type_at(move.to_square)

                                    if moved_piece == QUEEN and first_queen_move:
                                        features['queen_moved_at'] = board.fullmove_number
                                        first_queen_move = False

                                    if captured_piece == QUEEN:
                                        features['queen_changed_at'] = board.fullmove_number

                                    if move.promotion:
                                        features['promotion'] += 1
                                    if board.is_check():
                                        features['total_checks'] += 1
                                        if first_check:
                                            features['first_check_at'] = board.fullmove_number
                                            first_check = False  
                                    best_move = 0                
                                    new_data["BestMove"].append([int(k["pv"][0].uci()==move_list[i+1]) for k in info])
                                    new_data["BestMoveScores"].append([k["score"].white().score() for k in info])
                                    new_data["ValidMoveCount"].append(board.legal_moves.count())
                                except IndexError:
                                    pass
                                except KeyError:
                                    new_data["BestMove"].append(None)
                                    new_data["BestMoveScores"].append(None)
                                except Exception as eeeee:
                                    new_data["MoveScore"].append(None)
                                    new_data["BestMove"].append(None)
                                    new_data["BestMoveScores"].append(None)
                                    print("Error evaluating: "+ new_data["PGN"])
                                    break
                            try:
                                if board.is_checkmate():
                                    features['is_checkmate'] += 1
                                if board.is_stalemate():
                                    features['is_stalemate'] += 1
                                if board.is_insufficient_material():
                                    features['insufficient_material'] += 1
                                if board.can_claim_draw():
                                    features['can_claim_draw'] += 1
                                features['total_moves'] = board.fullmove_number
                                features['end_pieces'] = len(board.piece_map())
                                new_data["game_features"] = dict(features)
                            except Exception as e:
                                pass
                            save_thread = Thread(target=save_data, args=(new_data, line_no)) #shouldn't get interrupted
                            save_thread.start()
                            save_thread.join()
                        if not new_data["EventNo"]%10:
                            speed[1][speed[0]%5] = 10/(time.time()-speed_timer)
                            speed[0] += 1
                            speed_timer = time.time()
                            progress(counter-data[0]["EventNo"], last_game-data[0]["EventNo"], str(counter-data[0]["EventNo"])+" processed. Speed:" + str(round(sum(speed[1])/5,3))+"gps. ETA: "+display_time((last_game-counter)/(sum(speed[1])/5)))
                        new_data = {"EventNo":0, "WhiteElo":0, "BlackElo":0, "PGN": "", "MoveScore": [], "MoveScoreCP": [], "BestMove": [], "BestMoveScores":[], "ValidMoveCount":[], "GameFeatures":{}, "Result": ""}                            
                    first_loop = False
                    new_data["EventNo"] = counter
                    if counter>=last_game:
                        break
                    counter += 1
                if new_data["BlackElo"] and new_data["WhiteElo"]:
                    if line == "":
                        pgn = not pgn
                    if pgn:
                        new_data["PGN"] += line    
                
                try:
                    if "WhiteElo" in line:
                        new_data["WhiteElo"]=int(re.search("\d+", line).group())
                    if "BlackElo" in line:
                        new_data["BlackElo"]=int(re.search("\d+", line).group())
                except AttributeError: 
                    new_data["WhiteElo"]=-1
                    new_data["BlackElo"]=-1
        except Exception as e:
            print("\nAn unhandled exception occurred: {}".format(type(e))) 
            print("Stopping operation")
            save_thread.join()
            with open("bookmarkk.txt", "w") as file:
                file.write("Bookmark: ("+str(last_starting_line)+","+str(data[-1]["EventNo"]+1)+")")
            print("Bookmark: ("+str(last_starting_line)+","+str(data[-1]["EventNo"]+1)+")")
            print("Total number of processed games:", data[-1]["EventNo"]-data[0]["EventNo"]+1)
            print("Total runtime:", display_time(time.time()-start_time))
            raise e
        except KeyboardInterrupt:
            print("Stopping operation")
            save_thread.join()
    
    if len(data)-last_save>=1: # save for each 15 game
        with open(save_file, "ab") as s_file:
            pickle.dump(data[-(len(data)-last_save):], s_file)
    with open("bookmarkk.txt", "w") as file:
        file.write("Bookmark: ("+str(last_starting_line)+","+str(data[-1]["EventNo"]+1)+")")
    print("Bookmark: ("+str(last_starting_line)+","+str(data[-1]["EventNo"]+1)+")")
    print("Total number of processed games:", len(data))
    print("Total runtime:", display_time(time.time()-start_time))
    
    
def parse_args():
    """
    Define an argument parser and return the parsed arguments
    """
    parser = argparse.ArgumentParser(
        prog='annotator',
        description='takes chess games in a PGN file and prints '
        'annotations to standard output')
    parser.add_argument("--threads", "-t",
                        help="threads for use by the engine \
                            (default: %(default)s)",
                        type=int,
                        default=12)
    parser.add_argument("--bookmark", "-b", help="Bookmark for resuming session(first number)",
                        type=int,
                        default=0)
    parser.add_argument("--counter", "-c", help="Bookmark  for resuming session(second number)",
                        type=int,
                        default=1)
    parser.add_argument("--last_game", "-l", help="Analyze up to this game",
                        type=int,
                        default=36600)  
    parser.add_argument("--engine_path", "-e",
                        help="analysis engine path(default: %(default)s)",
                        type=str,
                        default="stockfish_13_win_x64_bmi2\\stockfish_9_x64_bmi2.exe")
    return parser.parse_args()
 
if __name__ == "__main__":
    args = parse_args()
    main(threads=args.threads, bookmark=args.bookmark, counter=args.counter, last_game=args.last_game, engine_path=args.engine_path)
    print("Saving the results")
    with open("uniform_analyzed.pickle", "wb") as s_file:
        pickle.dump(data, s_file)
    print("Saved as uniform_analyzed.pickle")
    print("Can safely close this now, sometimes stucks here.")