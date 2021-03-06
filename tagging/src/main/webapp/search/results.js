define(["backbone", "handlebars", "text!search/results.hbs", "text!search/resultsListing.hbs"],
function(BB,HBS, resultsTemplate, resultsListingTemplate){
	return BB.View.extend({
		initialize: function(opts){
			this.results = opts.results;
			this.template = HBS.compile(resultsTemplate);
			this.listingTemplate = HBS.compile(resultsListingTemplate);
			var tags = [];
			_.each(_.keys(this.results.tags),function(tag){ 
				if(!/^phs\d{6}\.v\d+$/.test(tag)){
					tags.push(
						{"required": false,
						"excluded":false,
						"name": tag,
						"count":this.results.tags[tag]}
					);					
				}
			}.bind(this));
			this.results.tags = tags;
			_.each(this.results.studies, function(study, studyName){
				study.guiId = studyName.replace(".","_");
				study.study_name = study[_.keys(study)[0]].study_description;
			});
			this.activeStudy = _.keys(this.results.studies)[0];
			HBS.registerHelper("makeIdSafe", this.makeIdSafe);
			HBS.registerHelper("sizeObject", function(value){
				return _.keys(value).length;
			});
			HBS.registerHelper("listTags", function(dt){
				return _.uniq([].concat.apply([], dt.results.map(
					function(result){
						return result.metadata_tags.concat(result.value_tags);})
					)).filter(function(value){
						return !(value.startsWith("PHV")||value.startsWith("pht")||value.startsWith("phs"));
					}).sort().join(', ');
			});
			this.render();
		},
		events:{
			"click .filter-toggle-required": "reset",
			"click .filter-toggle-require": "require",
			"click .filter-toggle-exclude": "exclude",
			"click .filter-toggle-excluded": "reset",
			"click .dt-search-results-toggle": "toggleResults",
			"click .dt-filter-toggle": "filterByDt",
			"click .dt-download-toggle": "addDtToExport",
			"click .var-filter-toggle": "filterByVariable",
			"click .var-download-toggle": "addVariableToExport",
			"shown.bs.tab .study-results-tab": "updateActiveStudy"
		},
		tagName: "div",
		render: function(){
			var focusedElement = document.activeElement.id;
			this.update();
			this.$el.html(this.template(this.results));
			$('#' + this.activeStudy.replace(".","_") + "-tab")[0].classList.add("active");
			$('#' + this.activeStudy.replace(".","_"))[0].classList.add("active");
			$('#' + this.activeStudy.replace(".","_"))[0].classList.add("show");
			
			_.defer(function(){
				$('#search-results-listing').append(this.listingTemplate(this.results.resultsWithTableMeta));
			}.bind(this));
			if(focusedElement!==""){
				$("#"+focusedElement).focus();	
			}
		},
		
		makeIdSafe: function(value){
			return value.replace(".","_");
		},
		toggleResults: function(event){
			$('#dt-search-results-'+this.makeIdSafe(event.target.dataset["dtid"])).toggle();;
			$('.dt-search-results-chevron-'+this.makeIdSafe(event.target.dataset["dtid"])).toggle();;
		},
		updateActiveStudy: function(event){
			this.activeStudy = event.target.dataset["studyid"];
			this.render();
		},
		findTarget: function(event){
			var target = undefined;			
			var targetName = event.currentTarget.id.substring(20);
			if(event.currentTarget.classList.contains("tag")){
				target = _.find(this.results.tags, function(tag){
					return tag.name === targetName;
				}.bind(this));
			}
			return target;	
		},
		require: function(event){
			var  target = this.findTarget(event);
			target.required = true;
			target.excluded = false;
			this.render();
		},
		exclude: function(event){
			var  target = this.findTarget(event);
			target.required = false;
			target.excluded = true;
			this.render();
		},
		reset: function(event){
			var  target = this.findTarget(event);
			target.required = false;
			target.excluded = false;
			this.render();
		},
		update: function(){
			this.updateTags();
			this.filter();
		},
		updateTags: function(){
			var requiredTags = [];
			var tags = [];
			var excludedTags = [];
			this.results.tags.forEach(function(tag){
				if(tag.required){
					requiredTags.push(tag);
				}else if(tag.excluded){
					excludedTags.push(tag);
				}else{
					tags.push(tag);
				}
			});
			requiredTags.sort();
			tags.sort();
			excludedTags.sort();
			this.results.tagStates = {
				requiredTags:requiredTags.length > 0 ? requiredTags : false, 
				tags:tags.length > 0 ? tags : false, 
				excludedTags:excludedTags.length > 0 ? excludedTags : false
			};
		}
	});
});