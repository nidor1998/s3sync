-- Callback function name should be `filter`
function filter(source_object)
    return string.find(source_object.key, "dir21/")
end