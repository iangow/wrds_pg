get_dirs <- function(path = ".") {
    dirs <- list.dirs(path)
    dirs <- dirs[grepl("^\\./[^\\.]", dirs)]
    dirs <- gsub("^\\./", "", dirs)
    dirs
}

get_dirs()
