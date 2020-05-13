from os import listdir
from functools import reduce
from os.path import join as pjoin

NOT_ALLOWED = ' \n.,-:;?¿\'"'


def file_to_words_list(file_path):
    '''
    Receives the path of a file. Each line of the file contains one word.
    Return a list with the words.
    '''
    with open(file_path, 'r', encoding='utf-8') as file_obj:
        return words_list_from(file_obj)


def words_list_from(file_obj):
    return list(map(lambda word: word.strip(), file_obj))


def files_to_lists(directory, *files):
    '''
    Receives a directory. If file names are given, those will be processed.
    Otherwise all the files in the directory will be processed.
    Return a list of lists with words.
    '''
    if files: lst_files = files # specified files are processed
    else: lst_files = listdir(directory) # all files in directory are processed
    return list(map(lambda x: file_to_words_list(pjoin(directory, x)), lst_files))


def has_alpha_char(word):
    '''
    If word contains alphabet characters returns True. False otherwise.
    '''
    if word: return reduce(lambda a,b: a or b, map(lambda x: x.isalpha(), word))


class TextMetrics():

    def __init__(self, total_words, wrong_words, quality_score):
        self.total_words = total_words
        self.wrong_words = wrong_words
        self.quality_score = quality_score


class TextQualityAnalyzer:
    '''
    This class checks if the word exists in the provided words lists.
    '''

    def __init__(self, *words_lists):
        self.idiom_words = reduce(lambda lst1, lst2: lst1 + lst2, words_lists)
        self.text = ''
        self.__iter_words = None


    def __parse_text(self):
        self.__iter_words = map(lambda word: word.strip(NOT_ALLOWED), self.text.split(' '))


    def word_exist(self, word):
        '''
        If the word exist in the word list return True. False otherwise.
        '''
        word = word.lower()
        return word in self.idiom_words


    def __check_words_existance(self):
        wrong_words = 0
        total_words = 0
        for word in self.__iter_words:
            if has_alpha_char(word):
                wrong_words += int(not self.word_exist(word))
                total_words += 1
        return total_words, wrong_words


    def generate_metrics_obj(self, text):
        '''
        Receives a text and return an object with its metrics and score(int) depending of the existing words.
        '''
        self.text = text
        self.__parse_text()
        total_words, wrong_words = self.__check_words_existance()
        quality_score = total_words // (wrong_words + 1)
        return TextMetrics(total_words, wrong_words, quality_score)