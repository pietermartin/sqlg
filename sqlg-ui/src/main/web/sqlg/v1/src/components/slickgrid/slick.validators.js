(function ($) {
    // register namespace
    $.extend(true, window, {
        "Slick": {
            "Validators": {
                "RequiredTextValidator": RequiredTextValidator
            }
        }
    });

    function RequiredTextValidator(value) {
        if (value == null || value == undefined || !value.length) {
            return {valid: false, msg: "This is a required field"};
        } else {
            return {valid: true, msg: null};
        }
    }

})(jQuery);
