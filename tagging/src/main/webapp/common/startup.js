define(["jquery", "search/search", "bootstrap"],
	function($, search){
		return function(){
			new search({el:$('body')}).render();
		}
	});
