import os


def get_file_postfix(file_name: str = "") -> str:
    """

    :type file_name: 
    """
    return os.path.splitext(file_name)[-1]


def get_file_list_by_dir_path(root_dir_path: str = os.getcwd(), recursion: bool = False,
                              show_file: bool = True, show_dir: bool = True, only_show_postfix: bool = False,
                              sort_type: int = 0, sort_key=None, show_with_postfix_list: list = []) -> list:
    """ Param show_file displays the file
    :param root_dir_path: Folder path to search
    :param recursion: Whether you need to find the next level of data
    :param show_file: Show file or not
    :param show_dir: Whether to display directory
    :param only_show_postfix: Whether to show suffixes only
    :param sort_type: Sorting type
                    0 Don't order
                    1 Not reverse
                    2 Reverse
    :param sort_key: Sort the comparison element, function sort the key
    :param show_with_postfix_list: Need to show file suffixes
    :return: listed files


    TODO: Returns file/folder details
    """

    def add_with_path(sup_dir_path: str, sub_path: str):
        """ Add the found file path and ensure that the path does not repeat
        :param sup_dir_path:
        :param sub_path: 
        :return: 
        """
        if only_show_postfix:
            if os.path.isfile(os.path.join(sup_dir_path, sub_path)):
                file = get_file_postfix(sub_path)
        else:
            file = os.path.join(sup_dir_path, sub_path)

        if is_show_with_postfix:
            if get_file_postfix(sub_path) not in show_with_postfix_list:
                return

        if sup_dir_path == "" or path == "" or file == "":
            return

        if file not in result_set:
            result_set.add(file)

    result_set = set()
    try:
        if len(show_with_postfix_list) > 0 :
            is_show_with_postfix = True
        if not os.path.exists(root_dir_path) and os.path.isdir(root_dir_path):
            raise ValueError("can not find path or is not dir")
        for dir_path, dir_names, file_names in os.walk(root_dir_path):
            if show_file:
                for path in file_names:
                    add_with_path(dir_path, path)

            if show_dir and not only_show_postfix:
                for path in dir_names:
                    add_with_path(dir_path, path)

            if not recursion:
                break

    except Exception as e:
        print("\n".join(e.args))

    finally:
        result_list = list(result_set)
        if sort_type is 1:
            result_list.sort(key=sort_key)
        elif sort_type is 2:
            result_list.sort(key=sort_key, reverse=True)

        return result_list
