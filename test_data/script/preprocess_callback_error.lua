-- Callback function name should be `preprocess_callback`
function preprocess_callback(source_object, upload_metadata)
    error("This is a test error from preprocess_callback")

    return true, upload_metadata
end
