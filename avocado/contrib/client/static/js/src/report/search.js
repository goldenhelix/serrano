// report/search

require.def(
    
    'report/search',

    ['rest/datasource', 'rest/renderer', 'report/templates', 'lib/jquery.ui', 'lib/json2'],

    function(m_datasource, m_renderer, m_templates) {

        function init(shared) {

            var columns = $('#columns'),
                active_columns = $('#active-columns'),

                searchdialog = $('#search-dialog'),
                searchinput = $('#search'),
                searchform = $('form', searchdialog),
                searchbutton = $('#search-button');

            /*
             * Pre-setup and event handler binding
             */
            searchdialog.cache = {};

            searchdialog.get = function(id) {
                if (!searchdialog.cache[id]) {
                    var sel = '[data-model=column][data-id=' + id + ']'; 

                    searchdialog.cache[id] = {
                        'src': columns.find(sel),
                        'tgt': active_columns.find(sel)
                    };
                }
                return searchdialog.cache[id];
            };

            searchdialog.bind('addall.column', function(evt, id) {
                var category = $('[data-model=category][data-id=' + id + ']'),
                    columns = category.find('li');
                
                category.hide();
                for (var i=columns.length; i--;)
                    searchdialog.trigger('add.column', [$(columns[i]).attr('data-id')]);

                return false;
            });

            searchdialog.bind('add.column', function(evt, id) {
                map = searchdialog.get(id);

                map.src.removeClass('active');

                // check to see if this is the last child being 'activated',
                // if so then hide the category
                var sibs = map.src.siblings('.active:not(.filtered)');
                if (sibs.length == 0)
                    map.src.parents('[data-model=category]').hide();

                // detach from wherever it is and append it to the end of the
                // list. since non-active columns are not shown, this will be
                // perceived as being added to the end of the 'visible' list
                active_columns.append(
                    map.tgt.detach().addClass('active')
                );

                return false;
            });

            searchdialog.bind('remove.column', function(evt, id) {
                map = searchdialog.get(id);

                map.tgt.removeClass('active');
                map.src.addClass('active').parents('[data-model=category]').show();

                return false;
            });

            searchdialog.bind('search.column', function(evt, value) {
                searchinput.trigger('search', value);
                return false;
            });

            searchdialog.bind('save.column', function(evt) {
                var children = active_columns.children('.active'),
                    uri = searchdialog.attr('data-uri'),
                    ids = $.map(children, function(e, i) {
                        return parseInt($(e).attr('data-id'));
                    });

                var json = JSON.stringify({columns: ids});
                $.putJSON(uri, json, function() {
                    shared.report.trigger('update.report');
                });
            });


            var rnd = {
                columns: new m_renderer.template({
                    target: columns,
                    template: m_templates.columns
                }),
                active_columns: new m_renderer.template({
                    target: active_columns,
                    template: m_templates.active_columns
                })
            };

            var src = {
                columns: new m_datasource.ajax({
                    uri: searchform.attr('action'),
                    success: function(json) {
                        rnd.columns.render(json);
                        var columns = json.map(function(e) {
                            return e['columns'];
                        });
                        columns = Array.prototype.concat.apply([], columns);
                        rnd.active_columns.render(columns);

                        src.perspective.get();
                    }
                }),
                perspective: new m_datasource.ajax({
                    uri: searchdialog.attr('data-uri'),
                    success: function(json) {
                        if (json.store) {
                            var rcols = json.store.columns.reverse();
                            for (var i=0; i < rcols.length; i++)
                                searchdialog.trigger('add.column', [rcols[i]]);
                        }
                    }
                })
            };

            src.columns.get();

            searchinput.autocomplete({
                success: function(value, json) {
                    var objs = $('[data-model=column]', columns).addClass('filtered');
                    for (var i = 0; i < json.length; i++)
                        objs.jdata('id', json[i]).removeClass('filtered');

//                    rnd.criteria.target.html('<em class="ca mg">no results found for term "'+ value +'"</em>');
                }
            });

            columns.delegate('.add-column', 'click', function(evt) {
                var id = evt.target.hash.substr(1);
                searchdialog.trigger('add.column', [id]);
                return false;
            });

            columns.delegate('.add-all', 'click', function(evt) {
                var id = evt.target.hash.substr(1);
                searchdialog.trigger('addall.column', [id]);
                return false;
            });

            active_columns.delegate('.remove-column', 'click', function(evt) {
                var id = evt.target.hash.substr(1);
                searchdialog.trigger('remove.column', [id]);
                return false;
            });

            searchdialog.dialog({
                autoOpen: false,
                draggable: true,
                resizable: true,
                title: 'Show/Hide Columns',
                height: 550,
                width: 700,
                minWidth: 700,
                buttons: {
                    Cancel: function() {
                        searchdialog.dialog('close');
                    },
                    Save: function() {
                        searchdialog.trigger('save.column');
                        searchdialog.dialog('close');
                    }
                }
            });

            /*
             * The active columns list is sortable to easily define the order
             * of display of the columns. That is, the order defined in the
             * list (top-bottom) translates to the order in the table
             * (left-right).
             */
            active_columns.sortable({
                placeholder: 'placeholder',
                forcePlaceholderSize: true,
                forceHelperSize: true,
                opacity: 0.5,
                cursor: 'move',
                tolerance: 'intersect'
            });

            searchbutton.bind('click', function(evt) {
                searchdialog.dialog('open');
            });
        };

        return {init: init};
    });
//            var $columnSections = $('#column-div .column-section'),
//                $selColumnDiv = $('#sel-column-div'),
//                $selColumnList = $('#sel-column-list').sortable({
//                    placeholder: 'placeholder',
//                    forcePlaceholderSize: true,
//                    forceHelperSize: true,
//                    cursor: 'move',
//                    tolerance: 'intersect'
//                }).disableSelection(),
//
//
//                $columnSearchQ = $('#column-search-q').keyup(function() {  
//                    var q = SearchSanitizer.clean($columnSearchQ.val());
//
//                    clearTimeout($columnSearchQ.attr('timer'));
//
//                    if (q !== $columnSearchQ.attr('lastQ')) {
//                        $columnSearchQ.attr('timer', setTimeout(function() {
//                            $columnSearch.submit();
//                            $columnSearchQ.attr('lastQ', q);
//                        }, 500));
//                    }
//                }).
//
//                $columnSearch = $('#column-search').submit(function(evt) {
//                    evt.preventDefault();
//                    var q = $.trim($columnSearchQ.val().replace(/\s+/, ' ')),
//                        data = {'q': q == 'Search columns...' ? '' : q};
//
//                    $.get(this.action, $.param(data), function(json) {
//                        ajaxSuccess(json);
//                        var classes = json.column_ids.map(function(e) {
//                                return '.col' + e;
//                            }),
//                            classesStr = classes.join(','),
//                            $children = $columnSections.children();
//
//                        if (classes.length == 0) {
//                            $children.addClass('filtered');
//                        } else {                
//                            $children.not(classesStr).addClass('filtered');
//                            $children.filter(classesStr).removeClass('filtered');
//                        }
//
//                        for (var i = $columnSections.length; i--; ) {
//                            var $s = $($columnSections[i]),
//                                len = $s.children().not('.filtered, .inactive').length;
//
//                            if (len == 0) {
//                                $s.parent().addClass('hd');
//                            } else {
//                                $s.parent().removeClass('hd');
//                            }
//                        }
//                    });
//                }).submit();
//
//            ColumnManager = {
//                _aCache: {},
//                _yCache: {},
//                _groupCounts: (function() {
//                    var counts = {}, section, length;
//                    for (var i = $columnSections.length; i--; ) {
//                        section = $($columnSections[i]);
//                        length = section.children().not('.inactive').length;
//                        counts[section.attr('id')] = length;
//                    }
//                    return counts;
//                })(),
//                isPinned: null,
//                reCol: /col(\d+)/,
//            
//                getClass: function(className) {
//                    return '.'+className.match(this.reCol)[0];
//                },
//            
//                getObjs: function(cls) {
//                    var aObj = this._aCache[cls],
//                        yObj = this._yCache[cls];
//
//                    if (aObj === undefined) {
//                        aObj = $(cls, $columnSections);
//                        this._aCache[cls] = aObj;
//                    }
//                
//                    if (yObj === undefined) {
//                        yObj = $(cls, $selColumnList);
//                        this._yCache[cls] = yObj;
//                    }
//                
//                    return [aObj, yObj];
//                },
//            
//                setPinning: function() {
//                    var modalHeight = $columnEditor.height(),
//                        columnDivHeight = $selColumnDiv.height();
//                
//                    if (columnDivHeight > modalHeight) {
//                        if (this.isPinned === false)
//                            return;
//                            
//                        $selColumnDiv.removeClass('pinned');
//                        this.isPinned = false;
//                        // $columnEditor.bind('scroll', columnEditorScrollBind);
//                        // var scrollTop = $columnEditor.scrollTop();
//                        // $selColumnDiv.css('margin-top', scrollTop);
//                    } else {
//                        if (this.isPinned === true)
//                            return;
//                        $selColumnDiv.addClass('pinned');
//                        this.isPinned = true;
//                        // $columnEditor.unbind('scroll');
//                    }
//                },
//                
//                add: function(className) {
//                    /*
//                    ** Handles adding a column to the selected column list.
//                    */
//                    className = className || '';
//
//                    var cls = this.getClass(className),
//                        objSet = this.getObjs(cls),
//                        aObj = objSet[0],
//                        yObj = objSet[1];
//
//                    aObj.addClass('inactive');
//
//                    yObj.detach().removeClass('inactive')
//                        .appendTo($selColumnList);
//                
//                    if (this.isPinned)
//                        this.setPinning();
//                    
//                    var parent = aObj.parent(),
//                        gparent = parent.parent();
//
//                    if (--this._groupCounts[parent.attr('id')] == 0 && !gparent.hasClass('hd'))
//                        gparent.addClass('hd');        },
//            
//                addMany: function(classNames) {
//                    classNames = classNames || [];
//                
//                    for (var cls, i = 0, len = classNames.length; i < len; i++)
//                        this.add(classNames[i]);
//                },
//            
//                remove: function(className) {
//                    /*
//                    ** Handles removing a column from the selected column list.
//                    */
//                    className = className || '';
//
//                    var cls = this.getClass(className),
//                        objSet = this.getObjs(cls),
//                        aObj = objSet[0],
//                        yObj = objSet[1];
//
//                    aObj.removeClass('inactive');
//                    yObj.addClass('inactive');            
//
//                    if (!this.isPinned)
//                        this.setPinning();
//
//                    var parent = aObj.parent(),
//                        gparent = parent.parent();
//
//                    if (++this._groupCounts[parent.attr('id')] > 0 && gparent.hasClass('hd'))
//                        gparent.removeClass('hd');
//                    
//                },
//            
//                removeMany: function(classNames) {
//                    classNames = classNames || [];
//                
//                    for (var i = 0, len = classNames.length; i < len; i++)
//                        this.remove(classNames[i]);
//                }
//            };
//
//
//            // set initial pinning based on loaded columns
//            
//
//            $('.add-category').click(function(evt) {
//                evt.preventDefault();
//                // this does not add filtered out columns 
//                var classNames = [],
//                    items = $(this.hash).children().not('.filtered, .inactive');
//
//                // early exit
//                if (items.length == 0)
//                    return;
//
//                for (var i = 0, len = items.length; i < len; i++)
//                    classNames.push(items[i].className);
//
//                ColumnManager.addMany(classNames);
//            });
//
//
//            $('.add-column').click(function(evt) {
//                evt.preventDefault();
//                ColumnManager.add($(this).parent().attr('className'));
//            });
//
//            $('.remove-column').click(function(evt) {
//                evt.preventDefault();
//                ColumnManager.remove($(this).parent().attr('className'));
//            });
//
//            $('#remove-all').click(function(evt) {
//                evt.preventDefault();
//                var classNames = [],
//                    items = $selColumnList.children().not('.inactive, .locked');
//
//                // early exit
//                if (items.length == 0)
//                    return;
//
//                for (var i = 0, len = items.length; i < len; i++)
//                    classNames.push(items[i].className);
//
//                ColumnManager.removeMany(classNames);        
//            });
//         
//            $('.open-column-editor').click(function(evt) {
//                evt.preventDefault();
//                evt.stopPropagation();
//                $columnEditor.dialog('open');
//                if (ColumnManager.isPinned === null)
//                    ColumnManager.setPinning();
//            });
//
//        };
