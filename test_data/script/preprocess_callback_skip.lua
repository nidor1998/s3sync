-- Callback function name should be `preprocess_callback`
function preprocess_callback(source_object, upload_metadata)
    return false, upload_metadata
end
