import synonyms

def testWordExit(wordexit):
    # 测试synonyms中是否存在这个词
    synonyms.display(wordexit)


def testWordVec(word):
    # 得出synonyms中这个词的向量
    wordvector=synonyms.v(word)
    # print(wordvector)
    return wordvector

def compareWord(source,destitation):
    # 比较两个词的相似度
    sim=synonyms.compare(source,destitation)
    print(sim)


if __name__ == '__main__':
    labels= ['经济','文學','鲁迅','小说','社会']
    labelsVectors=[]
    for i in range(len(labels)):
        try:
            wordVector = testWordVec(labels[i])
        except KeyError as err:
            print('第', i, '个词不存在词袋中')
            continue
        print('第',i,'个词向量为：')
        print(wordVector)
        print(len(wordVector))
        labelsVectors.append(wordVector)
    print(len(labelsVectors))
    print(labelsVectors)