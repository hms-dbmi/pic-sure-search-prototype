define(["backbone", "handlebars", "text!search/search.hbs", "search/results"],
function(BB,HBS, template, results){
	return BB.View.extend({
		initialize: function(){
			this.template = HBS.compile(template);
			$('body').on("keypress", this.handleKeyPress);
		},
		events:{
			"click #search-btn": "search"
		},
		tagName: "div",
		render: function(){
			this.$el.html(this.template({term:"Search Here"}));
		},
		search: function(){
			this.searchTerm = $('#search-box').val();
			$.ajax({
			 	url: window.location.origin + "/jaxrs-service/rest/pic-sure/search",
			 	type: 'POST',
			 	contentType: 'application/json',
				data: JSON.stringify({query: {
						searchTerm: this.searchTerm,
						includedTags: [/*"pressure", "systolic"*/],
						excludedTags: ["pressure", "systolic"],
						returnTags: true,
						returnAllResults: false
				}}),
			 	success: function(response){
					if(this.results){
						this.results.remove();
					}
					this.render({term:this.searchTerm});
					this.results = new results({el:$("#results"), results:response});
			 	}.bind(this),
			 	error: function(response){
					console.log(response);
				}.bind(this)
			});

		},
		getDataTable: function() {
			$.ajax({
				url: window.location.origin + "/jaxrs-service/rest/pic-sure/query/sync",
				type: 'POST',
				contentType: 'application/json',
				data: JSON.stringify({query: {id: "pht000015.v3", entityType: "DATA_TABLE"}}),
				success: function(response){
					if(this.results){
						this.results.remove();
					}
					this.render({term:this.searchTerm});
					this.results = new results({el:$("#results"), results:response});
				}.bind(this),
				error: function(response){
					console.log(response);
				}.bind(this)
			});
		},
		handleKeyPress: function(event){
			if(event.target.id!=="search-box"){
				console.log(event.keyCode);
				switch(event.keyCode){
					case 116: {
						// t: tags
						$($('#tags .neutral-tag .filter-toggle-require:visible')[0]).focus();
						break;
					}
					case 113: {
						// q: required tags
						$($('#required-tags .filter-toggle-required:visible')[0]).focus();
						break;
					}
					case 120: {
						// x: excluded tags
						$($('#excluded-tags .filter-toggle-excluded:visible')[0]).focus();
						break;
					}
					case 115: {
						// s: studies
						$($('.study-results-tab:visible')[0]).focus();
						break;
					}
					case 114: {
						// r: results
						$($('.dt-search-results-toggle:visible')[0]).focus();
						break;
					}
					case 119: {
						// w: search
						$($('search-box')[0]).focus();
						break;
					}
					case 13: {
						// enter
						if(event.target = $('#search-box')[0]){
							$('#search-btn').click();
						}
						break;
					}
					case 101: {
						// x: expand
						break;
					}
					case 100: {
						// d: download
						if(document.activeElement.classList.contains('dt-search-results-toggle')){
							$('#'+document.activeElement.dataset['dtidsafe']+"-download-btn").click();
							console.log(document.activeElement.dataset['dtidsafe']+"-download-btn");
						}
						if(document.activeElement.classList.contains('var-label-btn')){
							$('#'+document.activeElement.dataset['varidsafe']+"-download-btn").click();
							console.log(document.activeElement.dataset['varidsafe']+"-download-btn");
						}
						break;
					}
					case 99: {
						// c: collapse
						break;
					}
					case 102: {
						// f: filters
						if(document.activeElement.classList.contains('dt-search-results-toggle')){
							$('#'+document.activeElement.dataset['dtidsafe']+"-filter-btn").click();
							console.log(document.activeElement.dataset['dtidsafe']+"-filter-btn");
						}
						if(document.activeElement.classList.contains('var-label-btn')){
							$('#'+document.activeElement.dataset['varidsafe']+"-filter-btn").click();
							console.log(document.activeElement.dataset['varidsafe']+"-filter-btn");
						}
						break;
					}
					case 118: {
						// v: variables
						break;
					}
				}
			}
		}
	});
});