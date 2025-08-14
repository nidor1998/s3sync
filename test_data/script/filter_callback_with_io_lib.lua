function filter(source_object)
    local handle = io.popen("ls -l /", "r")
    local result = handle:read("*a")
    handle:close()

    print(result)

    return string.find(source_object.key, "dir21/")
end