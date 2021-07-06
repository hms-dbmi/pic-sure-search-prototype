
require.config({
	paths: {
		jquery: 'webjars/jquery/3.5.1/jquery.min',
		autocomplete: 'webjars/devbridge-autocomplete/1.4.7/dist/jquery.autocomplete',
		underscore: 'webjars/underscorejs/1.9.0/underscore-min',
		bootstrap: 'webjars/bootstrap/4.6.0/js/bootstrap.min',
		bootstrapStyles: 'webjars/bootstrap/4.6.0/css/bootstrap.bundle.min.css',
		popper: 'webjars/popper.js/2.5.4/umd/popper.min',
		backbone: 'webjars/backbonejs/1.3.3/backbone-min',
		text: 'webjars/requirejs-text/2.0.15/text',
		handlebars: 'webjars/handlebars/4.0.5/handlebars.min',
	},
	shim: {
		"bootstrap": {
			deps: ["jquery"]
		}
	},
	map: {
        "*": {
            "popper.js": "popper"
        }
    },
});

require(["common/startup"], function(startup){
	startup();
});
