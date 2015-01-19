function orderby(touples)

  local function mapper(rec)
     local element = map()
    element["shoeSize"] = rec["shoeSize"];
    element["shoeColour"] = rec["shoeColour"]
    return element
  end 
  
  local function accumulate(currentList, nextElement)
    local shoeColour = nextElement["shoeColour"]
    info("current:"..tostring(currentList).." next:"..tostring(nextElement))
      if currentList[shoeColour] == nil then
        currentList[shoeColour] = list()
      end 
      list.append(currentList[shoeColour], nextElement)
      return currentList
  end
  
  local function mymerge(a, b)
    return list.merge(a, b)
  end
  
  local function reducer(this, that)
    return map.merge(this, that, mymerge)
  end
  
  return touples:map(mapper):aggregate(map{}, accumulate):reduce(reducer)
end