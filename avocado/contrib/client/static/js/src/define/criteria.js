require.def('define/criteria', ['define/templates'], function(templates) {
    var tmpl = $.jqotec(templates.scope_element);
    
    var Criteria = function(criteria_constraint, uri){
        var element = $($.jqote(tmpl, {pk:criteria_constraint.concept_id,
                                       description:"Generic English Sentence Representing Concept " + criteria_constraint.concept_id,
                                       uri:uri+criteria_constraint.concept_id}));
        element.data("constraint", criteria_constraint);
        
        element.find(".remove-criterion").click(function(){
            element.trigger("CriteriaRemovedEvent");
        });
        
        function showCriteria(){
            var evt = $.Event("ShowConceptEvent");
            evt.constraints = element.data("constraint");
            element.trigger(evt);
        }
        // Display the concept in the main area when the user clicks on the description
        element.find(".field-anchor").click(showCriteria);
        
        return element.addClass("selected");
    };

    return {Criteria:Criteria};
});
